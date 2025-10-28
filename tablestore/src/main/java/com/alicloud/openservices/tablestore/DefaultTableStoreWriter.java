package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.utils.ParamChecker;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.*;
import com.alicloud.openservices.tablestore.writer.config.BucketConfig;
import com.alicloud.openservices.tablestore.writer.dispatch.*;
import com.alicloud.openservices.tablestore.writer.Group;
import com.alicloud.openservices.tablestore.writer.handle.WriterHandleStatistics;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeNotRetryStrategy;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeRetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultTableStoreWriter implements TableStoreWriter {
    private static Logger logger = LoggerFactory.getLogger(TableStoreWriter.class);

    private static final int SCHEDULED_CORE_POOL_SIZE = 2;

    private final AsyncClientInterface ots;

    private final Executor executor;

    private final WriterConfig writerConfig;

    private TableStoreCallback<RowChange, ConsumedCapacity> callback;

    private TableStoreCallback<RowChange, RowWriteResult> resultCallback;

    private final String tableName;

    private TableMeta tableMeta;

    private Bucket[] buckets;

    private final WriterHandleStatistics writerStatistics;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private BaseDispatcher dispatcher;

    private final Semaphore semaphore;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(SCHEDULED_CORE_POOL_SIZE, new ThreadFactory() {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "writer-scheduled-pool-%d" + counter.getAndIncrement());
        }
    });

    private final boolean isInnerConstruct;


    /**
     * Secondary index: Duplicate PrimaryKey is not allowed in batch requests.
     */
    private boolean allowDuplicatePkInBatchRequest = true;

    public DefaultTableStoreWriter(AsyncClientInterface ots, String tableName, WriterConfig config, TableStoreCallback<RowChange, ConsumedCapacity> callback, Executor executor) {
        Preconditions.checkNotNull(ots, "The ots client can not be null.");
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The table name can not be null or empty.");
        Preconditions.checkNotNull(executor, "The executor service can not be null.");
        this.writerStatistics = new WriterHandleStatistics();
        this.ots = ots;
        this.tableName = tableName;
        this.writerConfig = config;
        this.callback = callback;
        this.resultCallback = createResultCallback(callback);
        this.executor = executor;
        this.allowDuplicatePkInBatchRequest = writerConfig.isAllowDuplicatedRowInBatchRequest();
        semaphore = new Semaphore(writerConfig.getConcurrency());
        isInnerConstruct = false;

        initialize();
        closed.set(false);
    }

    public DefaultTableStoreWriter(
            String endpoint,
            ServiceCredentials credentials,
            String instanceName,
            String tableName,
            WriterConfig config,
            TableStoreCallback<RowChange, RowWriteResult> resultCallback) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The table name can not be null or empty.");
        this.writerStatistics = new WriterHandleStatistics();

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
        this.ots = new AsyncClient(endpoint, new DefaultCredentialProvider(credentials), instanceName, cc, new ResourceManager(cc, null));
        this.tableName = tableName;
        this.writerConfig = config;
        this.callback = null;
        this.resultCallback = resultCallback;
        this.executor = createThreadPool(config);
        this.allowDuplicatePkInBatchRequest = writerConfig.isAllowDuplicatedRowInBatchRequest();
        semaphore = new Semaphore(writerConfig.getConcurrency());
        isInnerConstruct = true;

        initialize();
        closed.set(false);
    }

    public DefaultTableStoreWriter(
            String endpoint,
            ServiceCredentials credentials,
            String instanceName,
            String tableName,
            WriterConfig config,
            ClientConfiguration cc,
            TableStoreCallback<RowChange, RowWriteResult> resultCallback) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The table name can not be null or empty.");
        this.writerStatistics = new WriterHandleStatistics();
        this.ots = new AsyncClient(endpoint, new DefaultCredentialProvider(credentials), instanceName, cc, new ResourceManager(cc, null));
        this.tableName = tableName;
        this.writerConfig = config;
        this.callback = null;
        this.resultCallback = resultCallback;
        this.executor = createThreadPool(config);
        this.allowDuplicatePkInBatchRequest = writerConfig.isAllowDuplicatedRowInBatchRequest();
        semaphore = new Semaphore(writerConfig.getConcurrency());
        isInnerConstruct = true;

        initialize();
        closed.set(false);
    }


    private void initialize() {
        logger.info("Start initialize ots writer, table name: {}.", tableName);
        DescribeTableRequest request = new DescribeTableRequest();
        request.setTableName(tableName);
        Future<DescribeTableResponse> result = ots.describeTable(request, null);
        DescribeTableResponse res = null;
        try {
            res = result.get();
        } catch (Exception e) {
            throw new ClientException(e);
        }
        if (res.getIndexMeta() != null && res.getIndexMeta().size() > 0) {
            allowDuplicatePkInBatchRequest = false;
            logger.info("Table [{}] has globalIndex, allowDuplicatePkInBatchRequest will be overwrite by [false]", tableName);
        }
        this.tableMeta = res.getTableMeta();
        logger.info("End initialize with table meta: {}.", tableMeta);

        buckets = new Bucket[writerConfig.getBucketCount()];
        for (int i = 0; i < writerConfig.getBucketCount(); i++) {
            BucketConfig bucketConfig = new BucketConfig(
                    i,
                    this.tableMeta.getTableName(),
                    this.writerConfig.getWriteMode(),
                    this.allowDuplicatePkInBatchRequest);

            buckets[i] = new Bucket(bucketConfig, ots, writerConfig, resultCallback, executor, writerStatistics, semaphore);
        }

        switch (writerConfig.getDispatchMode()) {
            case HASH_PARTITION_KEY:
                dispatcher = new HashPartitionKeyDispatcher(writerConfig.getBucketCount());
                break;
            case ROUND_ROBIN:
                dispatcher = new RoundRobinDispatcher(writerConfig.getBucketCount());
                break;
            case HASH_PRIMARY_KEY:
                dispatcher = new HashPrimaryKeyDispatcher(writerConfig.getBucketCount());
                break;
            default:
                throw new ClientException(String.format("The dispatch mode [%s] not supported", writerConfig.getDispatchMode()));
        }

        startFlushTimer(writerConfig.getFlushInterval());
        startLogTimer(writerConfig.getLogInterval());
    }

    /**
     * Based on the number of cores in the user's machine, an appropriate thread pool is internally constructed (N being the number of cores).
     * core/max:    Configurable by the user; default is number of cores + 1.
     * blockQueue:  Configurable by the user; default is 1024.
     * Reject:      CallerRunsPolicy
     */
    private ExecutorService createThreadPool (WriterConfig config) {
        int coreThreadCount = config.getCallbackThreadCount();
        int maxThreadCount = coreThreadCount;
        int queueSize = config.getCallbackThreadPoolQueueSize();

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "writer-callback-" + counter.getAndIncrement());
            }
        };

        return new ThreadPoolExecutor(coreThreadCount, maxThreadCount, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(queueSize), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private TableStoreCallback<RowChange,RowWriteResult> createResultCallback(final TableStoreCallback<RowChange,ConsumedCapacity> callback) {
        if (callback != null) {
            return new TableStoreCallback<RowChange, RowWriteResult>() {
                @Override
                public void onCompleted(RowChange req, RowWriteResult res) {
                    callback.onCompleted(req, res.getConsumedCapacity());
                }

                @Override
                public void onFailed(RowChange req, Exception ex) {
                    callback.onFailed(req, ex);
                }
            };
        } else {
            return null;
        }
    }

    @Override
    public void addRowChange(RowChange rowChange) {
        if (writerConfig.isEnableSchemaCheck()) {
            ParamChecker.checkRowChange(tableMeta, rowChange, writerConfig);
        }

        Group group = new Group(1);
        while (true) {
            if (!addRowChangeInternal(rowChange, group)) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException exp) {
                }
            } else {
                break;
            }
        }
    }

    @Override
    public Future<WriterResult> addRowChangeWithFuture(RowChange rowChange) {
        if (writerConfig.isEnableSchemaCheck()) {
            ParamChecker.checkRowChange(tableMeta, rowChange, writerConfig);
        }

        Group group = new Group(1);
        while (true) {
            if (!addRowChangeInternal(rowChange, group)) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException exp) {
                }
            } else {
                break;
            }
        }

        return group.getFuture();
    }

    @Override
    public boolean tryAddRowChange(RowChange rowChange) {
        if (writerConfig.isEnableSchemaCheck()) {
            ParamChecker.checkRowChange(tableMeta, rowChange, writerConfig);
        }

        Group group = new Group(1);
        return addRowChangeInternal(rowChange, group);
    }

    private boolean addRowChangeInternal(RowChange rowChange, final Group group) {
        if (closed.get()) {
            throw new ClientException("The writer has been closed.");
        }

        int targetBucketIndex = dispatcher.getDispatchIndex(rowChange);

        return buckets[targetBucketIndex].addRowChange(rowChange, group);
    }

    public void startFlushTimer(int flushInterval) {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                triggerFlush();
            }
        }, 0, flushInterval, TimeUnit.MILLISECONDS);
    }

    private void startLogTimer(int interval) {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                StringBuilder ringBufferRemain = new StringBuilder("RingBuffer Remain: ");
                for (Bucket bucket : buckets) {
                    ringBufferRemain.append(bucket.getRingBuffer().remainingCapacity());
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

    @Override
    public void addRowChange(List<RowChange> rowChanges, List<RowChange> dirtyRows) throws ClientException {
        dirtyRows.clear();
        for (RowChange rowChange : rowChanges) {
            try {
                addRowChange(rowChange);
            } catch (ClientException e) {
                dirtyRows.add(rowChange);
            }
        }
        if (!dirtyRows.isEmpty()) {
            throw new ClientException("There is dirty rows.");
        }
    }

    @Override
    public Future<WriterResult> addRowChangeWithFuture(List<RowChange> rowChanges) throws ClientException {
        Group group = new Group(rowChanges.size());
        for (RowChange rowChange : rowChanges) {
            if (writerConfig.isEnableSchemaCheck()) {
                ParamChecker.checkRowChange(tableMeta, rowChange, writerConfig);
            }

            try {
                while (true) {
                    if (!addRowChangeInternal(rowChange, group)) {
                        try {
                            Thread.sleep(1);
                        } catch (InterruptedException exp) {
                        }
                    } else {
                        break;
                    }
                }
            } catch (ClientException e) {
                group.failedOneRow(rowChange, e);
            }
        }

        return group.getFuture();
    }

    @Override
    public void setCallback(final TableStoreCallback<RowChange, ConsumedCapacity> callback) {
        this.callback = callback;
        this.resultCallback = createResultCallback(callback);
        for (Bucket bucket : buckets) {
            bucket.setResultCallback(resultCallback);
        }
    }

    @Override
    public void setResultCallback(TableStoreCallback<RowChange, RowWriteResult> resultCallback) {
        this.callback = null;
        this.resultCallback = resultCallback;
        for (Bucket bucket : buckets) {
            bucket.setResultCallback(resultCallback);
        }
    }

    @Override
    public TableStoreCallback<RowChange, ConsumedCapacity> getCallback() {
        return this.callback;
    }

    @Override
    public TableStoreCallback<RowChange, RowWriteResult> getResultCallback() {
        return this.resultCallback;
    }


    @Override
    public WriterConfig getWriterConfig() {
        return writerConfig;
    }

    @Override
    public WriterStatistics getWriterStatistics() {
       return writerStatistics;
    }

    private CountDownLatch triggerFlush() {
        CountDownLatch latch = new CountDownLatch(writerConfig.getBucketCount());
        for (Bucket bucket : buckets) {
            bucket.addSignal(latch);
        }
        logger.info("WriterStatistics: " + writerStatistics);
        return latch;
    }

    @Override
    public synchronized void flush() throws ClientException {
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
    public synchronized void close() {
        if (closed.get()) {
            throw new ClientException("The writer has already been closed.");
        }

        flush();
        scheduledExecutorService.shutdown();
        for (Bucket bucket : buckets) {
            bucket.close();
            logger.debug(String.format("bucket [%d] is closed.", bucket.getId()));
        }

        /**
         * The internally built client and executor need to be shut down internally, without user awareness.
         */
        if (isInnerConstruct) {
            ots.shutdown();
            ((ExecutorService)executor).shutdown();
        }
        closed.set(true);
    }
}
