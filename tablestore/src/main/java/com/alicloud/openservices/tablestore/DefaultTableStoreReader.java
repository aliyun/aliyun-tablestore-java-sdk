package com.alicloud.openservices.tablestore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowQueryCriteria;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.reader.PrimaryKeyWithTable;
import com.alicloud.openservices.tablestore.reader.ReaderBucket;
import com.alicloud.openservices.tablestore.reader.ReaderDispatcher;
import com.alicloud.openservices.tablestore.reader.ReaderEvent;
import com.alicloud.openservices.tablestore.reader.ReaderGroup;
import com.alicloud.openservices.tablestore.reader.ReaderResult;
import com.alicloud.openservices.tablestore.reader.ReaderStatistics;
import com.alicloud.openservices.tablestore.reader.ReaderUtils;
import com.alicloud.openservices.tablestore.reader.RowReadResult;
import com.alicloud.openservices.tablestore.reader.TableStoreReaderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultTableStoreReader implements TableStoreReader {
    private static final int SCHEDULED_CORE_POOL_SIZE = 2;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(SCHEDULED_CORE_POOL_SIZE, new ThreadFactory() {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "reader-scheduled-pool-%d" + counter.getAndIncrement());
        }
    });
    private final Logger logger = LoggerFactory.getLogger(DefaultTableStoreReader.class);
    private final AsyncClientInterface ots;
    private final TableStoreReaderConfig config;
    private final TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback;
    private final Executor executor;
    private final ReaderBucket[] buckets;
    private final Semaphore semaphore;
    private final ReaderStatistics statistics;
    private final ReaderDispatcher dispatcher;
    private final Map<String, TableMeta> metaMap;

    public DefaultTableStoreReader(AsyncClientInterface ots, TableStoreReaderConfig config, Executor executor, TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback) {
        this.ots = ots;
        this.config = config;
        this.executor = executor;
        this.callback = callback;
        this.statistics = new ReaderStatistics();

        semaphore = new Semaphore(config.getConcurrency());
        metaMap = new HashMap<String, TableMeta>();

        this.buckets = new ReaderBucket[config.getBucketCount()];
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new ReaderBucket(ots, semaphore, config, callback, executor, statistics);
        }
        dispatcher = new ReaderDispatcher(buckets.length);

        startFlushTimer(config.getFlushInterval());
        startLogTimer(config.getLogInterval());
    }

    @Override
    public void addPrimaryKey(String tableName, PrimaryKey primaryKey) {
        if (config.isCheckTableMeta()) {
            checkPrimaryKeyWithTable(tableName, primaryKey);
        }

        ReaderGroup group = new ReaderGroup(1);
        PrimaryKeyWithTable primaryKeyWithTable = new PrimaryKeyWithTable(tableName, primaryKey);
        while (true) {
            if (!addPrimaryKeyWithTableInternal(primaryKeyWithTable, group)) {
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
    public Future<ReaderResult> addPrimaryKeyWithFuture(String tableName, PrimaryKey primaryKey) {
        if (config.isCheckTableMeta()) {
            checkPrimaryKeyWithTable(tableName, primaryKey);
        }

        ReaderGroup group = new ReaderGroup(1);
        PrimaryKeyWithTable primaryKeyWithTable = new PrimaryKeyWithTable(tableName, primaryKey);
        while (true) {
            if (!addPrimaryKeyWithTableInternal(primaryKeyWithTable, group)) {
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
    public void addPrimaryKeys(String tableName, List<PrimaryKey> primaryKeys) {
        ReaderGroup group = new ReaderGroup(primaryKeys.size());
        for (PrimaryKey primaryKey : primaryKeys) {
            if (config.isCheckTableMeta()) {
                checkPrimaryKeyWithTable(tableName, primaryKey);
            }
            PrimaryKeyWithTable primaryKeyWithTable = new PrimaryKeyWithTable(tableName, primaryKey);
            while (true) {
                if (!addPrimaryKeyWithTableInternal(primaryKeyWithTable, group)) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException exp) {
                    }
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public Future<ReaderResult> addPrimaryKeysWithFuture(String tableName, List<PrimaryKey> primaryKeys) {
        ReaderGroup group = new ReaderGroup(primaryKeys.size());
        for (PrimaryKey primaryKey : primaryKeys) {
            if (config.isCheckTableMeta()) {
                checkPrimaryKeyWithTable(tableName, primaryKey);
            }
            PrimaryKeyWithTable primaryKeyWithTable = new PrimaryKeyWithTable(tableName, primaryKey);
            while (true) {
                if (!addPrimaryKeyWithTableInternal(primaryKeyWithTable, group)) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException exp) {
                    }
                } else {
                    break;
                }
            }
        }

        return group.getFuture();
    }

    @Override
    public void setRowQueryCriteria(RowQueryCriteria rowQueryCriteria) {
        for (ReaderBucket bucket : buckets) {
            bucket.setRowQueryCriteria(rowQueryCriteria);
        }
    }

    @Override
    public void send() {
        logger.debug("trigger send data.");
        if (closed.get()) {
            throw new ClientException("The reader has been closed.");
        }

        triggerEvent(ReaderEvent.EventType.SEND);

        logger.debug("user trigger send finished.");
    }

    @Override
    public void flush() {
        logger.debug("trigger flush and waiting.");
        if (closed.get()) {
            throw new ClientException("The reader has been closed.");
        }

        CountDownLatch latch = triggerEvent(ReaderEvent.EventType.FLUSH);
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ClientException(e);
        }
        logger.info("Reader statistics: " + statistics);
        logger.debug("user trigger flush finished.");
    }

    @Override
    public void setCallback(TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback) {
        for (ReaderBucket bucket : buckets) {
            bucket.setCallback(callback);
        }
    }

    private CountDownLatch triggerEvent(ReaderEvent.EventType type) {
        CountDownLatch latch = new CountDownLatch(1);
        for (ReaderBucket bucket : buckets) {
            bucket.addSignal(latch, type);
        }
        return latch;
    }

    public synchronized void close() {
        if (closed.get()) {
            throw new ClientException("The reader has already been closed.");
        }
        flush();

        scheduledExecutorService.shutdown();
        for (ReaderBucket bucket : buckets) {
            bucket.close();
        }
        closed.set(true);
    }

    private boolean addPrimaryKeyWithTableInternal(PrimaryKeyWithTable primaryKeyWithTable, final ReaderGroup readerGroup) {
        if (closed.get()) {
            throw new ClientException("The reader has been closed.");
        }

        int dispatchIndex = dispatcher.getDispatchIndex(primaryKeyWithTable.getPrimaryKey());
        return buckets[dispatchIndex].addPrimaryKeyWithTable(primaryKeyWithTable, readerGroup);
    }

    public void startFlushTimer(int flushInterval) {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                triggerEvent(ReaderEvent.EventType.FLUSH);
            }
        }, 0, flushInterval, TimeUnit.MILLISECONDS);
    }

    private void startLogTimer(int interval) {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                StringBuilder ringBufferRemain = new StringBuilder("RingBuffer Remain: ");
                for (ReaderBucket bucket : buckets) {
                    ringBufferRemain.append(bucket.getRingBuffer().remainingCapacity());
                    ringBufferRemain.append(", ");
                }
                logger.debug(ringBufferRemain.toString());
            }
        }, 0, interval, TimeUnit.MILLISECONDS);
    }

    private void checkPrimaryKeyWithTable(String tableName, PrimaryKey primaryKey) {
        if (metaMap.containsKey(tableName) && metaMap.get(tableName) == null) {
            throw new ClientException("The table : {" + tableName + "} does not exist.");
        }

        if (!metaMap.containsKey(tableName)) {
            try {
                DescribeTableResponse response = ots.asSyncClient().describeTable(new DescribeTableRequest(tableName));
                metaMap.put(tableName, response.getTableMeta());
            } catch (TableStoreException e) {
                metaMap.put(tableName, null);
                throw new ClientException("The table : {" + tableName + "} does not exist.");
            }
        }
        ReaderUtils.checkTableMeta(metaMap.get(tableName), primaryKey);
    }

    public ReaderStatistics getStatistics() {
        return statistics;
    }

    public TableStoreReaderConfig getConfig() {
        return config;
    }
}
