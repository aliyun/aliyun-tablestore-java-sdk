package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesBucket;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesWriterResult;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesWriterUtils;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesBucketConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.dispatch.TimeseriesBaseDispatcher;
import com.alicloud.openservices.tablestore.timeserieswriter.dispatch.TimeseriesHashPKDispatcher;
import com.alicloud.openservices.tablestore.timeserieswriter.dispatch.TimeseriesRoundRobinDispatcher;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesGroup;
import com.alicloud.openservices.tablestore.timeserieswriter.handle.TimeseriesWriterHandleStatistics;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeNotRetryStrategy;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeRetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @description： 基于 {@link AsyncTimeseriesClient}实现的时序表writer
 */
public class DefaultTableStoreTimeseriesWriter implements TableStoreTimeseriesWriter {
    private Logger logger = LoggerFactory.getLogger(DefaultTableStoreTimeseriesWriter.class);
    private static final int SCHEDULED_CORE_POOL_SIZE = 2;
    private TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics;
    private final AsyncTimeseriesClientInterface ots;
    private final TimeseriesWriterConfig timeseriesWriterConfig;
    private final Executor executor;
    private TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> resultCallback;
    private Boolean allowDuplicatePkInBatchRequest;
    private final Semaphore semaphore; //信号量 总量控制
    private Boolean isInnerConstruct;
    private TimeseriesBucket[] timeseriesBuckets;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private TimeseriesBaseDispatcher dispatcher;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(SCHEDULED_CORE_POOL_SIZE, new ThreadFactory() {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "time-series-writer-scheduled-pool-%d" + counter.getAndIncrement());
        }
    });

    /**
     * 时序表writer
     *
     * @param ots       异步时序表客户端实例
     * @param config    writer的配置
     * @param callback  行级别回调
     * @param executor  线程池
     */
    public DefaultTableStoreTimeseriesWriter(
            AsyncTimeseriesClientInterface ots,
            TimeseriesWriterConfig config,
            TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback,
            Executor executor) {
        Preconditions.checkNotNull(ots, "The ots client can not be null.");
        Preconditions.checkNotNull(executor, "The executor service can not be null.");
        this.timeseriesWriterHandleStatistics = new TimeseriesWriterHandleStatistics();
        this.ots = ots;
        this.timeseriesWriterConfig = config;
        this.resultCallback = createResultCallback(callback);
        this.executor = executor;
        this.allowDuplicatePkInBatchRequest = timeseriesWriterConfig.isAllowDuplicatedRowInBatchRequest();
        semaphore = new Semaphore(timeseriesWriterConfig.getConcurrency());
        isInnerConstruct = false;
        initialize();
        closed.set(false);

    }


    /**
     * 推荐使用的时序表writer
     *
     * @param endpoint       实例域名
     * @param credentials    认证信息：含AK，也支持token
     * @param instanceName   实例名
     * @param config         writer的配置
     * @param resultCallback 行级别回调
     */
    public DefaultTableStoreTimeseriesWriter(
            String endpoint,
            ServiceCredentials credentials,
            String instanceName,
            TimeseriesWriterConfig config,
            TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> resultCallback) {
        this.allowDuplicatePkInBatchRequest = true;
        this.timeseriesWriterHandleStatistics = new TimeseriesWriterHandleStatistics();
        ClientConfiguration cc = new ClientConfiguration();
        cc.setMaxConnections(config.getClientMaxConnections());
        switch (config.getWriterRetryStrategy()) {
            case CERTAIN_ERROR_CODE_NOT_RETRY:
                cc.setRetryStrategy(new CertainCodeNotRetryStrategy());
                break;
            case CERTAIN_ERROR_CODE_RETRY:
            default:
                cc.setRetryStrategy(new CertainCodeRetryStrategy());
        }

        this.ots = new AsyncTimeseriesClient(endpoint, credentials.getAccessKeyId(), credentials.getAccessKeySecret(), instanceName, cc, credentials.getSecurityToken());
        this.timeseriesWriterConfig = config;
        this.resultCallback = resultCallback;
        this.executor = this.createThreadPool(config);
        this.allowDuplicatePkInBatchRequest = this.timeseriesWriterConfig.isAllowDuplicatedRowInBatchRequest();
        this.semaphore = new Semaphore(this.timeseriesWriterConfig.getConcurrency());
        this.isInnerConstruct = true;
        this.initialize();
        this.closed.set(false);
    }

    /**
     * 推荐使用的时序表writer
     *
     * @param endpoint       实例域名
     * @param credentials    认证信息：含AK，也支持token
     * @param instanceName   实例名
     * @param config         writer的配置
     * @param cc             客户端的配置
     * @param resultCallback 行级别回调
     */
    public DefaultTableStoreTimeseriesWriter(
            String endpoint,
            ServiceCredentials credentials,
            String instanceName,
            TimeseriesWriterConfig config,
            ClientConfiguration cc,
            TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> resultCallback) {
        this.allowDuplicatePkInBatchRequest = true;
        this.timeseriesWriterHandleStatistics = new TimeseriesWriterHandleStatistics();
        this.ots = new AsyncTimeseriesClient(endpoint, credentials.getAccessKeyId(), credentials.getAccessKeySecret(), instanceName, cc, credentials.getSecurityToken());
        this.timeseriesWriterConfig = config;
        this.resultCallback = resultCallback;
        this.executor = this.createThreadPool(config);
        this.allowDuplicatePkInBatchRequest = this.timeseriesWriterConfig.isAllowDuplicatedRowInBatchRequest();
        this.semaphore = new Semaphore(this.timeseriesWriterConfig.getConcurrency());
        this.isInnerConstruct = true;
        this.initialize();
        this.closed.set(false);
    }


    private void initialize() {
        logger.info("Start initialize time series ots writer.");
        timeseriesBuckets = new TimeseriesBucket[timeseriesWriterConfig.getBucketCount()];
        for (int i = 0; i < timeseriesWriterConfig.getBucketCount(); i++) {
            TimeseriesBucketConfig timeseriesBucketConfig = new TimeseriesBucketConfig(
                    i,
                    this.timeseriesWriterConfig.getWriteMode(),
                    this.allowDuplicatePkInBatchRequest);

            timeseriesBuckets[i] = new TimeseriesBucket(timeseriesBucketConfig, ots, timeseriesWriterConfig,
                    resultCallback,
                    executor, timeseriesWriterHandleStatistics, semaphore);
        }
        switch (timeseriesWriterConfig.getDispatchMode()) {
            case HASH_PRIMARY_KEY:
                dispatcher = new TimeseriesHashPKDispatcher(timeseriesWriterConfig.getBucketCount());
                break;
            case ROUND_ROBIN:
                dispatcher = new TimeseriesRoundRobinDispatcher(timeseriesWriterConfig.getBucketCount());
                break;
            default:
                throw new ClientException(String.format("The dispatch mode [%s] not supported", timeseriesWriterConfig.getDispatchMode()));
        }

        startFlushTimer(timeseriesWriterConfig.getFlushInterval());
        startLogTimer(timeseriesWriterConfig.getLogInterval());
    }

    private ExecutorService createThreadPool(TimeseriesWriterConfig config) {
        int coreThreadCount = config.getCallbackThreadCount();
        int queueSize = config.getCallbackThreadPoolQueueSize();
        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            public Thread newThread(Runnable r) {
                return new Thread(r, "writer-callback-" + this.counter.getAndIncrement());
            }
        };
        return new ThreadPoolExecutor(coreThreadCount, coreThreadCount, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue(queueSize), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }


    public void startFlushTimer(int flushInterval) {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                triggerFlush();
            }
        }, 0, flushInterval, TimeUnit.MILLISECONDS);
    }


    private CountDownLatch triggerFlush() {
        CountDownLatch latch = new CountDownLatch(timeseriesWriterConfig.getBucketCount());
        for (TimeseriesBucket bucket : timeseriesBuckets) {
            bucket.addSignal(latch);
        }
        logger.info("TimeseriesWriterStatistics: " + timeseriesWriterHandleStatistics);
        return latch;
    }

    private void startLogTimer(int interval) {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                StringBuilder ringBufferRemain = new StringBuilder("RingBuffer Remain: ");
                for (TimeseriesBucket timeseriesBucket : timeseriesBuckets) {
                    ringBufferRemain.append(timeseriesBucket.getRingBuffer().remainingCapacity());
                    ringBufferRemain.append(", ");
                }
                logger.debug(ringBufferRemain.toString());

                StringBuilder dispatcherCount = new StringBuilder("Dispatcher Count: ");
                for (AtomicLong count : dispatcher.getBucketDispatchRowCount()) {
                    dispatcherCount.append(count.get());
                    dispatcherCount.append(", ");
                }
                logger.debug(dispatcherCount.toString());
            }
        }, 0, interval, TimeUnit.MILLISECONDS);
    }


    private TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> createResultCallback(final TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> callback) {
        if (callback != null) {
            return new TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult>() {
                @Override
                public void onCompleted(TimeseriesTableRow req, TimeseriesRowResult res) {
                    callback.onCompleted(req, res);
                }

                @Override
                public void onFailed(TimeseriesTableRow req, Exception ex) {
                    callback.onFailed(req, ex);
                }
            };
        } else {
            return null;
        }
    }


    @Override
    public void addTimeseriesRowChange(TimeseriesTableRow timeseriesTableRow) throws ClientException {
        checkTimeseriesTableRow(timeseriesTableRow);

        TimeseriesGroup timeseriesGroup = new TimeseriesGroup(1);
        while (true) {
            if (!addTimeseriesRowInternal(timeseriesTableRow, timeseriesGroup)) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException exp) {
                }
            } else {
                break;
            }
        }
    }

    public boolean addTimeseriesRowInternal(TimeseriesTableRow timeseriesTableRow, final TimeseriesGroup timeseriesGroup) {
        if (closed.get()) {
            throw new ClientException("The time series writer has been closed.");
        }

        int targetBucketIndex = dispatcher.getDispatchIndex(timeseriesTableRow.getTimeseriesRow());

        return timeseriesBuckets[targetBucketIndex].addRowChange(timeseriesTableRow, timeseriesGroup);
    }

    private void checkTimeseriesTableRow(TimeseriesTableRow timeseriesTableRow){
        // 检查tableName不为空
        Preconditions.checkArgument(timeseriesTableRow.getTableName() != null && !timeseriesTableRow.getTableName().isEmpty(), "The table name can not be null or empty.");
        // 检查时序数据主键的measurement不为空
        TimeseriesWriterUtils.checkMeasurement(timeseriesTableRow.getTimeseriesRow());
    }

    /**
     * 写入一条时序表数据
     *
     * @param timeseriesTableRow 时序表数据
     * @return 数据写入结果，如是否写入完成，是否写入成功，写入失败列等
     * @throws ClientException
     */
    @Override
    public Future<TimeseriesWriterResult> addTimeseriesRowChangeWithFuture(TimeseriesTableRow timeseriesTableRow) throws ClientException {
        checkTimeseriesTableRow(timeseriesTableRow);

        TimeseriesGroup timeseriesGroup = new TimeseriesGroup(1);
        while (true) {
            if (!addTimeseriesRowInternal(timeseriesTableRow, timeseriesGroup)) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException exp) {
                }
            } else {
                break;
            }
        }

        return timeseriesGroup.getFuture();
    }

    @Override
    public boolean tryAddTimeseriesRowChange(TimeseriesTableRow timeseriesTableRow) throws ClientException {
        checkTimeseriesTableRow(timeseriesTableRow);

        TimeseriesGroup timeseriesGroup = new TimeseriesGroup(1);
        return addTimeseriesRowInternal(timeseriesTableRow, timeseriesGroup);
    }

    @Override
    public void addTimeseriesRowChange(List<TimeseriesTableRow> timeseriesTableRows, List<TimeseriesTableRow> dirtyTimeseriesTableRows) throws ClientException {
        dirtyTimeseriesTableRows.clear();
        for (TimeseriesTableRow timeseriesTableRow : timeseriesTableRows) {
            checkTimeseriesTableRow(timeseriesTableRow);
            try {
                addTimeseriesRowChange(timeseriesTableRow);
            } catch (ClientException e) {
                dirtyTimeseriesTableRows.add(timeseriesTableRow);
            }
        }
        if (!dirtyTimeseriesTableRows.isEmpty()) {
            throw new ClientException("There is dirty rows.");
        }
    }

    /**
     * 批量写入时序表数据
     *
     * @param timeseriesTableRows 时序表数据List
     * @return 数据写入结果，如是否写入完成，是否写入成功，写入失败列等
     * @throws ClientException
     */
    @Override
    public Future<TimeseriesWriterResult> addTimeseriesRowChangeWithFuture(List<TimeseriesTableRow> timeseriesTableRows) throws ClientException {
        TimeseriesGroup timeseriesGroup = new TimeseriesGroup(timeseriesTableRows.size());
        for (TimeseriesTableRow timeseriesTableRow : timeseriesTableRows) {

            try {
                while (true) {
                    if (!addTimeseriesRowInternal(timeseriesTableRow, timeseriesGroup)) {

                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException exp) {
                        }
                    } else {
                        break;
                    }
                }
            } catch (ClientException e) {
                timeseriesGroup.failedOneRow(timeseriesTableRow, e);
            }
        }

        return timeseriesGroup.getFuture();
    }

    @Override
    public void setResultCallback(TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> resultCallback) {
        this.resultCallback = resultCallback;
        for (TimeseriesBucket bucket : timeseriesBuckets) {
            bucket.setResultCallback(resultCallback);
        }
    }


    @Override
    public TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> getResultCallback() {
        return resultCallback;
    }

    @Override
    public TimeseriesWriterConfig getTimeseriesWriterConfig() {
        return timeseriesWriterConfig;
    }

    @Override
    public TimeseriesWriterHandleStatistics getTimeseriesWriterStatistics() {
        return timeseriesWriterHandleStatistics;
    }

    @Override
    public void flush() throws ClientException {
        logger.debug("trigger flush and waiting.");
        if (closed.get()) {
            throw new ClientException("The writer has been closed.");
        }

        CountDownLatch latch = triggerFlush();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }
        logger.debug("user trigger flush finished.");

    }

    @Override
    public void close() {
        if (closed.get()) {
            throw new ClientException("The writer has already been closed.");
        }

        flush();
        scheduledExecutorService.shutdown();
        for (TimeseriesBucket timeseriesBucket : timeseriesBuckets) {
            timeseriesBucket.close();
            logger.debug(String.format("bucket [%d] is closed.", timeseriesBucket.getId()));
        }

        /**
         * 内部构建的client与executor需要内部shutdown，用户无感知
         */
        if (isInnerConstruct) {
            ots.shutdown();
            ((ExecutorService) executor).shutdown();
        }
        closed.set(true);
    }
}
