package com.alicloud.openservices.tablestore.tunnel.worker;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Custom configuration for TunnelWorker.
 */
public class TunnelWorkerConfig {
    private static final Logger LOG = LoggerFactory.getLogger(TunnelWorker.class);
    private static final int CORE_POOL_SIZE = 32;
    private static final int KEEP_ALIVE_SECONDS = 60;
    private static final int MAX_CORE_POOL_SIZE = 1000;
    private static final int BLOCKING_QUEUE_SIZE = 16;

    private static final int HEARTBEAT_MIN_INTERVAL_SEC = 5;
    private static final int MAX_RETRY_MILL_SEC_MIN = 200;

    /**
     * The timeout interval for Heartbeat. When a Heartbeat times out, the Tunnel server will consider the current TunnelClient unavailable (inactive), and the client needs to reconnect by calling ConnectTunnel again.
     */
    private long heartbeatTimeoutInSec = 300;
    /**
     * The interval for performing Heartbeat. Heartbeat is used for active Channel detection, Channel status updates, and initialization of (automated) data fetching tasks, etc.
     */
    private long heartbeatIntervalInSec = 30;
    /**
     * The time interval for users to perform the checkpoint operation (recording consumption position) on the Tunnel server after consuming the data, in units of ms.
     */
    private long checkpointIntervalInMillis = 5000;
    /**
     * The client's custom identifier, used to generate the Tunnel Client ID. Users can customize this parameter to distinguish between TunnelClients.
     */
    private String clientTag = System.getProperty("os.name");

    /**
     * The callback for processing data during user registration, including process and shutdown methods.
     */
    private IChannelProcessor channelProcessor;

    /**
     * Thread pool for reading data, which can be customized by users. When customizing, the number of threads in the thread pool should be as consistent as possible with the number of Channels in the Tunnel,
     * to ensure that each Channel can quickly be allocated computing resources (CPU).
     * In our default thread pool configuration, we have done the following to ensure throughput:
     * 1. Pre-allocate 32 core threads by default to ensure real-time throughput when the data is small (fewer Channels).
     * 2. The size of the working queue is appropriately reduced so that when the user's data volume is large (more Channels), it can more quickly trigger the thread pool's strategy to create new threads, thus promptly increasing more computing resources.
     * 3. A default thread keep-alive time is set (default 60 seconds), so when the data volume decreases, thread resources can be reclaimed in a timely manner.
     */
    private ThreadPoolExecutor readRecordsExecutor;
    /**
     * A thread pool for processing data, which can be user-defined. The description is consistent with the thread pool mentioned above.
     */
    private ThreadPoolExecutor processRecordsExecutor;

    /**
     * The maximum parallelism for Channel to read and process data, which can be used for memory control. The default value is -1, indicating no limit on the maximum parallelism.
     */
    private int maxChannelParallel = -1;
    private Semaphore maxChannelSemaphore;

    /**
     * The auxiliary thread pool of TunnelWorker, by default it uses CachedThreadPool.
     */
    private ThreadPoolExecutor channelHelperExecutor;

    private int maxRetryIntervalInMillis = 2000;

    /**
     * The configuration for each round of the Tunnel ReadRecords phase, including the maximum number of read rounds and the maximum amount of data to read.
     */
    private int readMaxTimesPerRound = 1;
    private int readMaxBytesPerRound = 4 * 1024 * 1024; // 4M bytes
    // TODO: Add max limit
    private static final int READ_MAX_TIMES_PER_ROUND = 50;
    private static final int READ_MAX_BYTES_PER_ROUND = 64 * 1024 * 1024;

    private boolean enableClosingChannelDetect = true;

    public TunnelWorkerConfig() {
        this(
                newDefaultThreadPool("read-records-executor-"),
                newDefaultThreadPool("process-records-executor-"),
                null
        );
    }

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
        Preconditions.checkArgument(heartbeatIntervalInSec >= HEARTBEAT_MIN_INTERVAL_SEC,
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

    public int getMaxRetryIntervalInMillis() {
        return maxRetryIntervalInMillis;
    }

    public void setMaxRetryIntervalInMillis(int maxRetryIntervalInMillis) {
        Preconditions.checkArgument(maxRetryIntervalInMillis >= MAX_RETRY_MILL_SEC_MIN,
                "max retry interval must bigger than or equal to %s mill seconds.", MAX_RETRY_MILL_SEC_MIN);
        this.maxRetryIntervalInMillis = maxRetryIntervalInMillis;
    }

    /**
     * Initialize the default thread pool. In our default thread pool configuration, we do the following to ensure throughput:
     * 1. Pre-allocate 32 core threads by default to ensure real-time throughput when the data size is small (fewer Channels).
     * 2. Appropriately reduce the size of the work queue so that when the user's data volume is large (more Channels), it can more quickly trigger the thread pool's strategy to create new threads, thus promptly scaling up more computational resources.
     * 3. Set a default thread keep-alive time (default 60 seconds) so that when the data volume decreases, thread resources can be timely reclaimed.
     *
     * @param threadPrefix: The prefix identifier for the thread name
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

    public int getMaxChannelParallel() {
        return maxChannelParallel;
    }

    public void setMaxChannelParallel(int maxChannelParallel) {
        this.maxChannelParallel = maxChannelParallel;
    }

    void setMaxChannelSemaphore(Semaphore maxChannelSemaphore) {
        this.maxChannelSemaphore = maxChannelSemaphore;
    }

    Semaphore getMaxChannelSemaphore() {
        return maxChannelSemaphore;
    }

    public void setChannelHelperExecutor(ThreadPoolExecutor channelHelperExecutor) {
        this.channelHelperExecutor = channelHelperExecutor;
    }

    public ThreadPoolExecutor getChannelHelperExecutor() {
        return channelHelperExecutor;
    }

    public int getReadMaxTimesPerRound() {
        return readMaxTimesPerRound;
    }

    public void setReadMaxTimesPerRound(int readMaxTimesPerRound) {
        this.readMaxTimesPerRound = readMaxTimesPerRound;
    }

    public int getReadMaxBytesPerRound() {
        return readMaxBytesPerRound;
    }

    public void setReadMaxBytesPerRound(int readMaxBytesPerRound) {
        this.readMaxBytesPerRound = readMaxBytesPerRound;
    }

    /**
     * Reclaim thread pool resources.
     */
    public void shutdown() {
        LOG.info("shutdown read records executor");
        readRecordsExecutor.shutdownNow();
        try {
            if (readRecordsExecutor.awaitTermination(100, TimeUnit.SECONDS)) {
                LOG.info("ReadRecords executor termination success.");
            } else {
                LOG.warn("ReadRecords executor termination until timeout");
            }
        } catch (InterruptedException e) {
            LOG.warn("Wait read records executor termination failed", e);
        }
        LOG.info("shutdown process records executor");
        processRecordsExecutor.shutdownNow();
        try {
            if (processRecordsExecutor.awaitTermination(100, TimeUnit.SECONDS)) {
                LOG.info("ProcessRecords executor termination success.");
            } else {
                LOG.warn("ProcessRecords executor termination until timeout");
            }
        } catch (InterruptedException e) {
            LOG.warn("Wait process records executor termination failed", e);
        }
        if (channelHelperExecutor != null) {
            channelHelperExecutor.shutdownNow();
            LOG.info("shutdown channel helper executor");
        }
    }

    public void setEnableClosingChannelDetect(boolean enableClosingChannelDetect) {
        this.enableClosingChannelDetect = enableClosingChannelDetect;
    }

    public boolean isEnableClosingChannelDetect() {
        return enableClosingChannelDetect;
    }
}
