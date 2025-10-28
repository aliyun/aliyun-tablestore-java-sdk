package com.alicloud.openservices.tablestore.writer.handle;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.*;
import com.alicloud.openservices.tablestore.writer.config.BucketConfig;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;



public class RowEventHandler implements EventHandler<RowChangeEvent> {
    private static Logger logger = LoggerFactory.getLogger(RowEventHandler.class);

    private AsyncClientInterface ots;
    private int concurrency;
    private int bucketConcurrency;
    private WriterConfig writerConfig;
    private BucketConfig bucketConfig;
    private TableStoreCallback<RowChange, RowWriteResult> callback;
    private Executor executor;
    private WriterHandleStatistics writerStatistics;
    private Semaphore callbackSemaphore;
    private Semaphore bucketSemaphore;

    private RequestManager requestManager;


    public RowEventHandler(
            AsyncClientInterface ots,
            BucketConfig bucketConfig,
            WriterConfig writerConfig,
            TableStoreCallback<RowChange, RowWriteResult> callback,
            Executor executor,
            WriterHandleStatistics writerStatistics,
            Semaphore semaphore) {
        this.ots = ots;
        this.concurrency = writerConfig.getConcurrency();
        this.bucketConfig = bucketConfig;
        this.callbackSemaphore = semaphore;
        this.callback = callback;
        this.executor = executor;
        this.writerStatistics = writerStatistics;
        this.writerConfig = writerConfig;


        switch (writerConfig.getWriteMode()) {
            case SEQUENTIAL:
                bucketConcurrency = 1;
                break;
            case PARALLEL:
            default:
                bucketConcurrency = concurrency;
                break;
        }
        bucketSemaphore = new Semaphore(bucketConcurrency);

        initRequestManager();
    }

    private void initRequestManager() {
        switch (writerConfig.getBatchRequestType()) {
            case BULK_IMPORT:
                requestManager = new BulkImportRequestManager(this.ots, writerConfig, this.bucketConfig, executor,
                        writerStatistics, callback, callbackSemaphore, bucketSemaphore);
                break;
            case BATCH_WRITE_ROW:
            default:
                requestManager = new BatchWriteRowRequestManager(this.ots, writerConfig, this.bucketConfig, executor,
                        writerStatistics, callback, callbackSemaphore, bucketSemaphore);
                break;
        }
    }

    public void setCallback(TableStoreCallback<RowChange, RowWriteResult> callback) {
        this.callback = callback;
        initRequestManager();
    }

    @Override
    public void onEvent(final RowChangeEvent rowChangeEvent, long sequence, boolean endOfBatch) throws Exception {

        boolean shouldWaitFlush = false;
        CountDownLatch latch = null;
        RequestWithGroups requestWithGroups = null;

        if (rowChangeEvent.type == RowChangeEvent.EventType.FLUSH) {
            logger.debug("FlushSignal with QueueSize: {}", requestManager.getTotalRowsCount());
            if (requestManager.getTotalRowsCount() > 0) {
                requestWithGroups = requestManager.makeRequest();
            }

            shouldWaitFlush = true;
            latch = rowChangeEvent.latch;
        } else {
            writerStatistics.totalRowsCount.incrementAndGet();
            final RowChange rowChange = rowChangeEvent.rowChange;
            final RowChangeWithGroup rowChangeWithGroup = new RowChangeWithGroup(rowChangeEvent.rowChange, rowChangeEvent.group);

            boolean succeed = requestManager.appendRowChange(rowChangeWithGroup);
            if (!succeed) {
                requestWithGroups = requestManager.makeRequest();
                succeed = requestManager.appendRowChange(rowChangeWithGroup);

                if (!succeed) {
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            writerStatistics.totalFailedRowsCount.incrementAndGet();
                            ClientException exception = new ClientException("Can not even append only one row into buffer.");
                            logger.error("RowChange Failed: ", exception);
                            rowChangeWithGroup.group.failedOneRow(rowChangeWithGroup.rowChange, exception);
                            if (callback != null) {
                                callback.onFailed(rowChange, exception);
                            }
                        }
                    });
                }
            }
        }

        if (requestWithGroups != null) {
            final RequestWithGroups finalRequestWithGroups = requestWithGroups;
            bucketSemaphore.acquire();      // First, block and wait for the bucket semaphore.
            callbackSemaphore.acquire();    // Post-block waiting for thread pool signals
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    writerStatistics.totalRequestCount.incrementAndGet();
                    requestManager.sendRequest(finalRequestWithGroups);
                }
            });
        }

        if (shouldWaitFlush) {
            bucketSemaphore.acquire(bucketConcurrency);
            bucketSemaphore.release(bucketConcurrency);
            logger.debug("Finish bucket waitFlush.");
            latch.countDown();
        }
    }

}
