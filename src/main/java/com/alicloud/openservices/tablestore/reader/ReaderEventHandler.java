package com.alicloud.openservices.tablestore.reader;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.RowQueryCriteria;
import com.alicloud.openservices.tablestore.reader.ReaderEvent.EventType;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderEventHandler implements EventHandler<ReaderEvent> {

    private final Logger logger = LoggerFactory.getLogger(ReaderEventHandler.class);

    private final AsyncClientInterface ots;
    private final TableStoreReaderConfig config;
    private final Executor executor;
    private final Semaphore callbackSemaphore;
    private final Semaphore bucketSemaphore;
    private final int bucketConcurrency;
    private final ReaderRequestManager requestManager;
    private final ReaderStatistics statistics;

    public ReaderEventHandler(
            AsyncClientInterface ots,
            TableStoreReaderConfig config,
            Executor executor,
            Semaphore callbackSemaphore,
            TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback,
            ReaderStatistics statistics) {
        this.ots = ots;
        this.config = config;
        this.executor = executor;
        this.callbackSemaphore = callbackSemaphore;
        this.bucketSemaphore = new Semaphore(config.getConcurrency());
        this.bucketConcurrency = config.getConcurrency();
        this.statistics = statistics;

        requestManager = new ReaderRequestManager(ots, config, callbackSemaphore, callback, executor, bucketSemaphore, statistics);
    }

    @Override
    public void onEvent(ReaderEvent readerEvent, long l, boolean b) throws Exception {
        boolean shouldWaitFlush = false;
        CountDownLatch latch = null;
        ReqWithGroups reqWithGroups = null;

        if (readerEvent.type == ReaderEvent.EventType.FLUSH) {
            logger.debug("FlushSignal with QueueSize: {}", requestManager.getTotalPksCount());
            if (requestManager.getTotalPksCount() > 0) {
                reqWithGroups = requestManager.makeRequest();
            }
            shouldWaitFlush = true;
            latch = readerEvent.latch;
        } else if (readerEvent.type == EventType.SEND) {
            logger.debug("SendSignal with QueueSize: {}", requestManager.getTotalPksCount());
            if (requestManager.getTotalPksCount() > 0) {
                reqWithGroups = requestManager.makeRequest();
            }
        } else {
            statistics.totalRowsCount.incrementAndGet();
            final PrimaryKeyWithTable primaryKeyWithTable = readerEvent.pkWithTable;
            final PkWithGroup pkWithGroup = new PkWithGroup(primaryKeyWithTable, readerEvent.readerGroup);
            boolean succeed = requestManager.appendPrimaryKey(pkWithGroup);
            if (!succeed) {
                // 说明request size已达上限
                reqWithGroups = requestManager.makeRequest();
                requestManager.appendPrimaryKey(pkWithGroup);
            }
        }

        if (reqWithGroups != null) {
            final ReqWithGroups finalRequest = reqWithGroups;
            bucketSemaphore.acquire();      // 先阻塞等候桶信号量
            callbackSemaphore.acquire();    // 后阻塞等候线程池信号量
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    statistics.totalRequestCount.incrementAndGet();
                    requestManager.sendRequest(finalRequest);
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

    public void setRowQueryCriteria(RowQueryCriteria rowQueryCriteria) {
        requestManager.setRowQueryCriteria(rowQueryCriteria);
    }

    public void setCallback(TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback) {
        requestManager.setCallback(callback);
    }
}
