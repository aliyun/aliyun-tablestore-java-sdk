package com.alicloud.openservices.tablestore.tunnel.worker;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TunnelClientInterface;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ShutdownTunnelRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alicloud.openservices.tablestore.core.ErrorCode.RESOURCE_GONE;

/**
 * TunnelWorker是基于TableStore数据接口之上的全增量一体化服务，用户可以简单地实现对表中历史存量和新增数据的消费处理。
 * TunnelWorker的设计哲学是通过每一轮的定时心跳探测(Heartbeat)来进行活跃Channel的探测，Channel和ChannelConnect状态的更新，数据处理任务的初始化、运行和结束等。
 * TunnelWorker实现自动化数据处理的流程如下:
 * 1. TunnelWorker资源的初始化
 * 1.1 将TunnelWorker状态原子的由Ready置为Started(CAS操作)。
 * 1.2 根据TunnelWorkerConfig里的HeartbeatTimeout和ClientTag(客户端标识)等配置进行ConnectTunnel操作，和Tunnel服务端进行联通，
 * 并获取当前TunnelWorker对应的ClientId。
 * 1.3 初始化ChannelDialer(用于新建ChannelConnect), 每一个ChannelConnect都会和一个Channel一一对应，ChannelConnect上会记录
 * 有数据消费的位点。
 * 1.4 根据用户传入的处理数据的Callback和TunnelWorkerConfig中CheckpointInterval(向服务端记数据位点的间隔)
 * 包装出一个带自动记Checkpoint功能的数据处理器, 详细参见: ChannelProcessFactory。
 * 1.5 初始化TunnelStateMachine(会进行Channel状态机的自动化处理)。
 *
 * 2. 固定间隔进行Heartbeat，间隔由TunnelWorkerConfig里的heartbeatIntervalInSec参数决定。
 * 2.1 进行heartbeat请求，从Tunnel服务端获取最新可用的Channel列表，Channel中会包含有ChannelId, Channel的版本和Channel的状态信息。
 * 2.2 将服务端获取到的Channel列表和本地内存中的Channel列表进行Merge，然后进行ChannelConnect的新建和update，规则大致如下
 * 1) Merge: 相同ChannelId，认定版本号更大的为最新状态，直接进行覆盖，若未出现的Channel，则直接插入。
 * 2) 新建ChannelConnect: 若此Channel未新建有其对应的ChannelConnect，则会新建一个WAIT状态的ChannelConnect，若对应的Channel
 * 状态为OPEN状态，则同时会启动该ChannelConnect上的处理数据的循环流水线任务(ReadRecords&&ProcessRecords)，
 * 处理详细的细节可以参见ProcessDataPipeline。
 * 3) Update已有ChannelConnect: Merge完成后，若Channel对应的ChannelConnect存在，则根据相同ChannelId的Channel状态来更新
 * ChannelConnect的状态，比如Channel为Close状态也需要将ChannelConnect的状态置为Closed,进而终止处理任务的流水线任务，
 * 详细的细节可以参见ChannelConnect.notifyStatus方法。
 *
 * 3. 自动化的负载均衡和良好的水平扩展性
 * 运行多个TunnelWorker对同一个Tunnel进行消费时(TunnelId相同), 在TunnelWorker执行Heartbeat时，Tunnel服务端会自动的对Channel资源进行重分配，
 * 让活跃的Channel尽可能的均摊到每一个TunnelWorker上，达到资源负载均衡的目的。同时，在水平扩展性方面，用户可以很容易的通过增加TunnelWorker的
 * 数量来完成，TunnelWorker可以在同一个机器或者不同机器上。
 *
 * 4. 自动化的资源清理和容错处理
 * 4.1 资源清理: 当客户端(TunnelWorker)没有被正常shutdown时(比如异常退出或者手动结束)，我们会自动帮用户进行资源的回收，包括释放线程池，
 * 自动调用用户在Channel上注册的shutdown方法，关闭Tunnel连接等。
 * 4.2 容错处理: 当客户端出现Heartbeat超时等非参数类错误时，我们会自动帮用户Renew Connect，以保证数据消费可以稳定的进行持续同步。
 */
public class TunnelWorker implements ITunnelWorker {
    private static final Logger LOG = LoggerFactory.getLogger(TunnelWorker.class);
    private static final int CORE_POOL_SIZE = 2;
    private static final int WORKER_RANDOM_RETRY_MILLIS = 10000;

    private String tunnelId;
    private String clientId;
    private TunnelWorkerConfig workerConfig;
    private TunnelClientInterface client;
    private TunnelStateMachine stateMachine;
    private IChannelDialer channelDialer;
    private AtomicReference<TunnelWorkerStatus> workerStatus = new AtomicReference<TunnelWorkerStatus>();
    private Date lastHeartbeatTime;
    private ScheduledExecutorService heartbeatExecutor;

    public TunnelWorker(String tunnelId, TunnelClientInterface client, TunnelWorkerConfig workerConfig) {
        Preconditions.checkArgument(tunnelId != null && !tunnelId.isEmpty(),
            "The tunnel id should not be null or empty.");
        Preconditions.checkNotNull(client, "Tunnel client cannot be null.");
        Preconditions.checkNotNull(workerConfig, "Tunnel worker workerConfig cannot be null.");
        Preconditions.checkNotNull(workerConfig.getChannelProcessor(), "Channel Processor cannot be null.");

        init(tunnelId, client, workerConfig);
    }

    private void init(String tunnelId, TunnelClientInterface client, TunnelWorkerConfig config) {
        LOG.info("Initial tunnel worker, tunnelId: {}", tunnelId);
        this.tunnelId = tunnelId;
        this.client = client;
        this.workerConfig = config;
        this.workerStatus.set(TunnelWorkerStatus.WORKER_READY);
        this.lastHeartbeatTime = new Date();

        this.heartbeatExecutor = Executors.newScheduledThreadPool(CORE_POOL_SIZE, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "tunnel-heartbeat-scheduled-" + counter.getAndIncrement());
            }
        });
    }

    private void connect() {
        if (!workerStatus.compareAndSet(TunnelWorkerStatus.WORKER_READY, TunnelWorkerStatus.WORKER_STARTED)) {
            throw new ClientException(String.format("Tunnel worker has already been %s status", workerStatus));
        }
        while (true) {
            try {
                TunnelClientConfig conf = new TunnelClientConfig(workerConfig.getHeartbeatTimeoutInSec(),
                    workerConfig.getClientTag());
                ConnectTunnelRequest request = new ConnectTunnelRequest(tunnelId, conf);
                ConnectTunnelResponse resp = client.connectTunnel(request);
                this.clientId = resp.getClientId();
                // ConnectTunnel成功后，初始化TunnelWorker的其它依赖组件。
                this.channelDialer = new ChannelDialer(client, workerConfig);
                // 根据用户传入的处理数据的Callback和CheckpointInterval(向服务端记数据位点的间隔)
                // 包装出一个带自动记Checkpoint功能的数据处理器。
                IChannelProcessorFactory channelProcessorFactory = new ChannelProcessFactory(workerConfig);
                stateMachine = new TunnelStateMachine(tunnelId, clientId, channelDialer, channelProcessorFactory,
                    client);
                LOG.info("Connect tunnel success, clientId: {}, tunnelId: {}", clientId, tunnelId);
                break;
            } catch (TableStoreException te) {
                LOG.warn("Connect tunnel failed, tunnel id {}, error detail {}", tunnelId, te.toString());
                if (isTunnelInvalid(te.getErrorCode())) {
                    LOG.error("Tunnel is expired or invalid, tunnel worker will be halted.");
                    workerStatus.set(TunnelWorkerStatus.WORKER_HALT);
                    throw te;
                }
            } catch (Exception e) {
                LOG.warn("Connect tunnel failed, tunnel id {}, error detail {}", tunnelId, e.toString());
            }

            try {
                Thread.sleep(new Random(System.currentTimeMillis()).nextInt(WORKER_RANDOM_RETRY_MILLIS) + 1);
            } catch (Exception e) {
                LOG.warn("Reconnect worker error, error detail: {}", e.toString());
            }
        }
    }

    private class Heartbeat implements Runnable {
        @Override
        public void run() {
            // 若Worker处于ENDED状态，则需要重新进行资源初始化。
            if (workerStatus.get().equals(TunnelWorkerStatus.WORKER_ENDED)) {
                workerStatus.set(TunnelWorkerStatus.WORKER_READY);
                connect();
            }
            try {
                Date now = new Date();
                if (lastHeartbeatTime.compareTo(now) != 0) {
                    if (now.getTime() - lastHeartbeatTime.getTime() > TimeUnit.SECONDS.toMillis(
                        workerConfig.getHeartbeatTimeoutInSec())) {
                        LOG.error("Tunnel client heartbeat timeout, lastHeartbeatTime: {}.", lastHeartbeatTime);
                        throw new TableStoreException("tunnel client heartbeat timeout", RESOURCE_GONE);
                    }
                }
                LOG.info("Begin batch get channels.");
                List<Channel> currChannels = stateMachine.batchGetChannels();
                HeartbeatRequest request = new HeartbeatRequest(tunnelId, clientId, currChannels);
                HeartbeatResponse resp = client.heartbeat(request);
                lastHeartbeatTime = new Date();
                List<Channel> targetChannels = resp.getChannels();
                LOG.info("Begin batch update channels, num: {}, detail: {}.", targetChannels.size(),
                    channelsToString(targetChannels));
                stateMachine.batchUpdateChannels(targetChannels);
            } catch (TableStoreException te) {
                LOG.warn("Heartbeat error, TableStore Exception: {}.", te.toString());
                if (isTunnelInvalid(te.getErrorCode())) {
                    LOG.error("Tunnel is expired or invalid, tunnel worker will be halted.");
                    shutdown(true);
                } else {
                    shutdown(false);
                }
            } catch (Throwable e) {
                LOG.warn("Heartbeat error, Throwable: {}", e.toString());
                shutdown(false);
            }
        }
    }

    private boolean isTunnelInvalid(String errorCode) {
        return errorCode.equals(ErrorCode.TUNNEL_EXPIRED) || errorCode.equals(ErrorCode.INVALID_PARAMETER);
    }

    private String channelsToString(List<Channel> channels) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Channel channel : channels) {
            sb.append(channel);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void connectAndWorking() throws Exception {
        // 资源的一些初始化
        connect();
        // 定期进行Heartbeat, 进行活跃Channel的探测，Channel和ChannelConnect状态的更新，数据处理任务的初始化、运行和结束等。
        heartbeatExecutor.scheduleAtFixedRate(new Heartbeat(), 0, workerConfig.getHeartbeatIntervalInSec(),
            TimeUnit.SECONDS);

        // Add Shutdown hook for resource clear.
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                LOG.warn("Unexpected shutdown, do resources clear.");
                shutdown();
            }
        }
        ));
    }

    @Override
    public void shutdown() {
        shutdown(true);
    }

    /**
     * 关闭TunnelWorker。
     *
     * @param isHalt: TunnelWorker是否停止, true代表TunnelWorker所有相关资源(包括线程池)需要被关闭，false代表只是暂时的停止(可以通过重连恢复)。
     */
    private void shutdown(boolean isHalt) {
        if (workerStatus.get().equals(TunnelWorkerStatus.WORKER_ENDED) ||
            workerStatus.get().equals(TunnelWorkerStatus.WORKER_HALT)) {
            LOG.info("Tunnel worker has already been {} status, skip shutdown logic.", workerStatus);
            return;
        }
        if (channelDialer != null) {
            LOG.info("Shutdown channel dialer");
            channelDialer.shutdown();
        }
        if (stateMachine != null) {
            LOG.info("Shutdown tunnel state machine.");
            stateMachine.close();
        }
        if (isHalt && heartbeatExecutor != null) {
            LOG.info("Shutdown heartbeat executor.");
            heartbeatExecutor.shutdown();
        }
        try {
            LOG.info("Shutdown tunnel, tunnelId: {}, clientId: {}", tunnelId, clientId);
            client.shutdownTunnel(new ShutdownTunnelRequest(tunnelId, clientId));
        } catch (Exception e) {
            LOG.warn("Shutdown tunnel failed, tunnelId: {}, clientId: {}", tunnelId, clientId);
        }
        LOG.info("Tunnel worker is ended.");
        if (isHalt) {
            workerStatus.set(TunnelWorkerStatus.WORKER_HALT);
        } else {
            workerStatus.set(TunnelWorkerStatus.WORKER_ENDED);
        }
    }
}
