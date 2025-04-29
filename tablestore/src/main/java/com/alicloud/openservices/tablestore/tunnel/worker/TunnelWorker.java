package com.alicloud.openservices.tablestore.tunnel.worker;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TunnelClientInterface;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.tunnel.internal.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TunnelWorker is a full-incremental integrated service built on top of the TableStore data interface, allowing users to easily implement the consumption and processing of both historical and newly added data in a table.
 * The design philosophy of TunnelWorker is to detect active Channels through periodic heartbeat probes (Heartbeat), and to update the status of Channels and ChannelConnects, as well as initialize, run, and conclude data processing tasks.
 * The automated data processing workflow implemented by TunnelWorker is as follows:
 * 1. Initialization of TunnelWorker resources
 * 1.1 Set the state of TunnelWorker atomically from Ready to Started (CAS operation).
 * 1.2 Perform ConnectTunnel operations based on configurations such as HeartbeatTimeout and ClientTag (client identifier) in TunnelWorkerConfig to connect with the Tunnel server and obtain the ClientId corresponding to the current TunnelWorker.
 * 1.3 Initialize ChannelDialer (used for creating new ChannelConnects). Each ChannelConnect corresponds one-to-one with a Channel, and records the checkpoint of data consumption.
 * 1.4 Wrap a data processor with an automatic Checkpoint recording function according to the user-provided data processing Callback and the CheckpointInterval (interval for recording data checkpoints to the server) in TunnelWorkerConfig. For more details, see: ChannelProcessFactory.
 * 1.5 Initialize TunnelStateMachine (which will handle the automation of Channel state machines).
 * <p>
 * 2. Perform Heartbeat at fixed intervals, determined by the heartbeatIntervalInSec parameter in TunnelWorkerConfig.
 * 2.1 Perform a heartbeat request to get the latest available Channel list from the Tunnel server. The Channels will include ChannelId, Channel version, and Channel status information.
 * 2.2 Merge the Channel list obtained from the server with the local memory Channel list, then create and update ChannelConnects according to the following rules:
 * 1) Merge: For Channels with the same ChannelId, the one with the higher version number is considered the latest state and will be directly overwritten. If a Channel does not appear in the local list, it will be directly inserted.
 * 2) Create new ChannelConnect: If there is no existing ChannelConnect corresponding to this Channel, a new WAIT state ChannelConnect will be created. If the Channel's state is OPEN, the data processing cyclic pipeline task (ReadRecords && ProcessRecords) on the ChannelConnect will also be started. For more details, refer to ProcessDataPipeline.
 * 3) Update existing ChannelConnect: After merging, if a ChannelConnect exists for the corresponding Channel, its state will be updated according to the state of the Channel with the same ChannelId. For example, if the Channel is in the Close state, the ChannelConnect state will also be set to Closed, thereby terminating the pipeline task. For more details, see the ChannelConnect.notifyStatus method.
 * <p>
 * 3. Automated load balancing and good horizontal scalability
 * When multiple TunnelWorkers consume the same Tunnel (with the same TunnelId), during the execution of Heartbeat, the Tunnel server will automatically redistribute Channel resources so that active Channels are evenly distributed across each TunnelWorker, achieving resource load balancing. In terms of horizontal scalability, users can easily increase the number of TunnelWorkers, which can be deployed on the same machine or different machines.
 * <p>
 * 4. Automated resource cleanup and fault-tolerant handling
 * 4.1 Resource cleanup: When the client (TunnelWorker) is not properly shut down (e.g., abnormal exit or manual termination), we will automatically help users reclaim resources, including releasing thread pools, automatically calling the shutdown methods registered by users on the Channel, and closing Tunnel connections.
 * 4.2 Fault-tolerant handling: When the client encounters non-parameter errors such as Heartbeat timeouts, we will automatically Renew the connection for the user to ensure that data consumption can continue stably and continuously.
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
    private IChannelProcessorFactory factory;

    public TunnelWorker(
            String tunnelId,
            TunnelClientInterface client,
            TunnelWorkerConfig workerConfig) {
        this(tunnelId, client, workerConfig, null);
    }

    public TunnelWorker(
            String tunnelId,
            TunnelClientInterface client,
            TunnelWorkerConfig workerConfig,
            IChannelProcessorFactory factory) {
        Preconditions.checkArgument(tunnelId != null && !tunnelId.isEmpty(),
                "The tunnel id should not be null or empty.");
        Preconditions.checkNotNull(client, "Tunnel client cannot be null.");
        Preconditions.checkNotNull(workerConfig, "Tunnel worker workerConfig cannot be null.");

        this.factory = factory;
        // When there is no factory object, the ChannelProcessor needs to be explicitly set.
        if (this.factory == null) {
            Preconditions.checkNotNull(workerConfig.getChannelProcessor(), "Channel Processor cannot be null.");
        }

        init(tunnelId, client, workerConfig);
    }

    private void init(String tunnelId, TunnelClientInterface client, TunnelWorkerConfig config) {
        LOG.info("Initial tunnel worker, tunnelId: {}", tunnelId);
        this.tunnelId = tunnelId;
        this.client = client;
        if (config.getMaxChannelParallel() > 0) {
            config.setMaxChannelSemaphore(new Semaphore(config.getMaxChannelParallel(), true));
        }
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
                // After ConnectTunnel succeeds, initialize the other dependent components of TunnelWorker.
                this.channelDialer = new ChannelDialer(client, workerConfig);
                // According to the Callback for processing data and CheckpointInterval (the interval for recording data checkpoints to the server) provided by the user.
                // Wraps a data processor with automatic Checkpoint recording functionality.
                if (factory == null) {
                    factory = new ChannelProcessFactory(workerConfig);
                }
                stateMachine = new TunnelStateMachine(
                        tunnelId,
                        clientId,
                        channelDialer,
                        factory,
                        client);
                stateMachine.setEnableClosingChannelDetect(workerConfig.isEnableClosingChannelDetect());
                LOG.info("Connect tunnel success, RequestId: {}, clientId: {}, tunnelId: {}", resp.getRequestId(), clientId, tunnelId);
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
            // If the Worker is in the ENDED state, resources need to be re-initialized.
            if (workerStatus.get().equals(TunnelWorkerStatus.WORKER_ENDED)) {
                workerStatus.set(TunnelWorkerStatus.WORKER_READY);
                lastHeartbeatTime = new Date();
                connect();
            }
            try {
                Date now = new Date();
                if (lastHeartbeatTime.compareTo(now) != 0) {
                    if (now.getTime() - lastHeartbeatTime.getTime() > TimeUnit.SECONDS.toMillis(
                            workerConfig.getHeartbeatTimeoutInSec())) {
                        LOG.error("Tunnel client heartbeat timeout, lastHeartbeatTime: {}.", lastHeartbeatTime);
                        shutdown(false);
                        return;
                    }
                }
                LOG.info("Begin batch get channels.");
                List<Channel> currChannels = stateMachine.batchGetChannels();
                HeartbeatRequest request = new HeartbeatRequest(tunnelId, clientId, currChannels);
                HeartbeatResponse resp = client.heartbeat(request);
                lastHeartbeatTime = new Date();
                List<Channel> targetChannels = resp.getChannels();
                LOG.info("Begin batch update channels, RequestId: {}, num: {}, detail: {}.", resp.getRequestId(),
                        targetChannels.size(), channelsToString(targetChannels));
                stateMachine.batchUpdateChannels(targetChannels);
            } catch (TableStoreException te) {
                LOG.warn("Heartbeat error, TableStore Exception: {}.", te.toString());
                if (isTunnelInvalid(te.getErrorCode())) {
                    LOG.error("Tunnel is expired or invalid, tunnel worker will be halted.");
                    shutdown(true);
                }
            } catch (Throwable e) {
                LOG.warn("Heartbeat error, Throwable: {}", e.toString());
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
        // Some initialization of resources
        connect();
        // Periodically perform Heartbeat to detect active Channels, update the status of Channel and ChannelConnect, and initialize, run, and complete data processing tasks.
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
     * Close the TunnelWorker.
     *
     * @param isHalt: Whether the TunnelWorker should stop. True means all related resources of the TunnelWorker (including thread pools) need to be closed, while false means it is just a temporary stop (can be resumed through reconnection).
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
            heartbeatExecutor.shutdownNow();
            try {
                if (heartbeatExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.info("Heartbeat executor termination success.");
                } else {
                    LOG.warn("Heartbeat executor termination until timeout");
                }
            } catch (InterruptedException e) {
                LOG.warn("Wait heartbeat executor termination failed", e);
            }
        }
        try {
            LOG.info("Shutdown tunnel, tunnelId: {}, clientId: {}", tunnelId, clientId);
            client.shutdownTunnel(new ShutdownTunnelRequest(tunnelId, clientId));
        } catch (Exception e) {
            LOG.warn("Shutdown tunnel failed, tunnelId: {}, clientId: {}", tunnelId, clientId, e);
        }
        LOG.info("Tunnel worker is ended.");
        if (isHalt) {
            workerStatus.set(TunnelWorkerStatus.WORKER_HALT);
        } else {
            workerStatus.set(TunnelWorkerStatus.WORKER_ENDED);
        }
    }
}
