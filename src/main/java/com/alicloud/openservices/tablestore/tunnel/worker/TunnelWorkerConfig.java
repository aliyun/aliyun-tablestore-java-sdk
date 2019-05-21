package com.alicloud.openservices.tablestore.tunnel.worker;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TunnelWorker的自定义配置。
 */
public class TunnelWorkerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(TunnelWorker.class);
    private static final int CORE_POOL_SIZE = 32;
    private static final int KEEP_ALIVE_SECONDS = 60;
    private static final int MAX_CORE_POOL_SIZE = 1000;
    private static final int BLOCKING_QUEUE_SIZE = 16;

    private static final int HEARTBEAT_MIN_INTERVAL_SEC = 5;

    /**
     * Heartbeat的超时间隔，当Heartbeat发生超时，Tunnel服务端会认为当前TunnelClient不可用(失活)，客户端需要重新的进行ConnectTunnel。
     */
    private long heartbeatTimeoutInSec = 300;
    /**
     * 进行Heartbeat的间隔，Heartbeat用于活跃Channel的探测，Channel状态的更新, (自动化)数据拉取任务的初始化等。
     */
    private long heartbeatIntervalInSec = 30;
    /**
     * 用户消费完数据后，向Tunnel服务端进行 记录消费位点操作(Checkpoint)的时间间隔，单位为ms。
     */
    private long checkpointIntervalInMillis = 5000;
    /**
     * 客户端的自定义标识，用于生成Tunnel Client ID, 用户可以自定义此参数来区分TunnelClient。
     */
    private String clientTag = System.getProperty("os.name");

    /**
     * 用户注册的处理数据的Callback，包括process和shutdown方法。
     */
    private IChannelProcessor channelProcessor;

    /**
     * 用于读数据的线程池，可支持用户自定义。在自定义时，线程池中的线程数要和Tunnel中的Channel数尽可能一致，
     * 这样可以保障每个Channel都能很快的分配到计算资源(CPU).
     * 在我们的默认线程池配置中，我们做了以下几件事，以保障吞吐量:
     * 1. 默认预发分配32个的核心线程，以保障数据较小时(Channel数较少时)的实时吞吐。
     * 2. 工作队列的大小适当调小，这样在用户数据量比较大(Channel数较多)时，可以更快的触发线程池新建线程的策略，及时的弹起更多的计算资源。
     * 3. 设置了默认的线程保活时间(默认60s)，当数据量下去后，可以及时回收线程资源。
     */
    private ThreadPoolExecutor readRecordsExecutor;
    /**
     * 用于处理数据的线程池，可支持用户自定义，说明和上面的线程池一致。
     */
    private ThreadPoolExecutor processRecordsExecutor;

    public TunnelWorkerConfig(IChannelProcessor processor) {
        this(
            newDefaultThreadPool("read-records-executor-"),
            newDefaultThreadPool("process-records-executor-"),
            processor
        );
    }

    public TunnelWorkerConfig(ThreadPoolExecutor readRecordsExecutor, ThreadPoolExecutor processRecordsExecutor,
                              IChannelProcessor processor) {
        this.readRecordsExecutor = readRecordsExecutor;
        this.processRecordsExecutor = processRecordsExecutor;
        this.channelProcessor = processor;
    }

    public long getHeartbeatTimeoutInSec() {
        return heartbeatTimeoutInSec;
    }

    public void setHeartbeatTimeoutInSec(long heartbeatTimeoutInSec) {
        Preconditions.checkArgument(heartbeatTimeoutInSec > heartbeatIntervalInSec,
            "heartbeat timeout should larger than heartbeat interval.");
        this.heartbeatTimeoutInSec = heartbeatTimeoutInSec;
    }

    public long getHeartbeatIntervalInSec() {
        return heartbeatIntervalInSec;
    }

    public void setHeartbeatIntervalInSec(long heartbeatIntervalInSec) {
        Preconditions.checkArgument(heartbeatIntervalInSec > HEARTBEAT_MIN_INTERVAL_SEC,
            "heartbeat interval must greater than or equal to %d seconds.", HEARTBEAT_MIN_INTERVAL_SEC);
        this.heartbeatIntervalInSec = heartbeatIntervalInSec;
    }

    public String getClientTag() {
        return clientTag;
    }

    public void setClientTag(String clientTag) {
        this.clientTag = clientTag;
    }

    public IChannelProcessor getChannelProcessor() {
        return channelProcessor;
    }

    public void setChannelProcessor(IChannelProcessor channelProcessor) {
        this.channelProcessor = channelProcessor;
    }

    public long getCheckpointIntervalInMillis() {
        return checkpointIntervalInMillis;
    }

    public void setCheckpointIntervalInMillis(long checkpointIntervalInMillis) {
        this.checkpointIntervalInMillis = checkpointIntervalInMillis;
    }

    public ThreadPoolExecutor getReadRecordsExecutor() {
        return readRecordsExecutor;
    }

    public void setReadRecordsExecutor(ThreadPoolExecutor readRecordsExecutor) {
        if (this.readRecordsExecutor != null) {
            this.readRecordsExecutor.shutdownNow();
        }
        this.readRecordsExecutor = readRecordsExecutor;
    }

    public ThreadPoolExecutor getProcessRecordsExecutor() {
        return processRecordsExecutor;
    }

    public void setProcessRecordsExecutor(ThreadPoolExecutor processRecordsExecutor) {
        if (this.processRecordsExecutor != null) {
            this.processRecordsExecutor.shutdownNow();
        }
        this.processRecordsExecutor = processRecordsExecutor;
    }

    /**
     * 初始化默认的线程池。在我们的默认线程池配置中，我们做了以下几件事，以保障吞吐量:
     * 1. 默认预发分配32个的核心线程，以保障数据较小时(Channel数较少时)的实时吞吐。
     * 2. 工作队列的大小适当调小，这样在用户数据量比较大(Channel数较多)时，可以更快的触发线程池新建线程的策略，及时的弹起更多的计算资源。
     * 3. 设置了默认的线程保活时间(默认60s)，当数据量下去后，可以及时回收线程资源。
     *
     * @param threadPrefix: 线程名称的前缀标识
     * @return
     */
    public static ThreadPoolExecutor newDefaultThreadPool(final String threadPrefix) {
        return new ThreadPoolExecutor(CORE_POOL_SIZE, MAX_CORE_POOL_SIZE, KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(BLOCKING_QUEUE_SIZE),
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger();

                @Override
                public Thread newThread(Runnable r) {
                    String threadName = threadPrefix + counter.getAndIncrement();
                    LOG.info("TunnelWorkerConfig new thread: " + threadName);
                    return new Thread(r, threadName);
                }
            },
            new CallerRunsPolicy());
    }

    /**
     * 回收线程池资源。
     */
    public void shutdown() {
        LOG.info("shutdown read records executor");
        readRecordsExecutor.shutdownNow();
        LOG.info("shutdown process records executor");
        processRecordsExecutor.shutdownNow();
    }
}
