package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.*;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RowChangeEventHandler implements EventHandler<RowChangeEvent> {
    private Logger logger = LoggerFactory.getLogger(RowChangeEventHandler.class);

    private AsyncClientInterface ots;
    private WriteRPCBuffer buffer;
    private int concurrency;
    private Semaphore semaphore;
    private TableStoreCallback<RowChange, RowWriteResult> callback;
    private Executor executor;
    private DefaultWriterStatistics writerStatistics;

    public RowChangeEventHandler(
            AsyncClientInterface ots, WriterConfig writerConfig,
            TableStoreCallback<RowChange, RowWriteResult> callback,
            Executor executor,
            DefaultWriterStatistics writerStatistics) {
        this.ots = ots;
        this.buffer = new WriteRPCBuffer(writerConfig);
        this.concurrency = writerConfig.getConcurrency();
        this.semaphore = new Semaphore(concurrency);
        this.callback = callback;
        this.executor = executor;
        this.writerStatistics = writerStatistics;
    }

    public void setCallback(TableStoreCallback<RowChange, RowWriteResult> callback) {
        this.callback = callback;
    }

    @Override
    public void onEvent(RowChangeEvent rowChangeEvent, long sequence, boolean endOfBatch) throws Exception {
        BatchWriteRowRequest request = null;

        boolean shouldWaitFlush = false;
        CountDownLatch latch = null;

        if (rowChangeEvent.type == RowChangeEvent.EventType.FLUSH) {
            logger.debug("FlushSignal with QueueSize: {}", buffer.getTotalRowsCount());
            if (buffer.getTotalRowsCount() > 0) {
                request = buffer.makeRequest();
                buffer.clear();
            }

            shouldWaitFlush = true;
            latch = rowChangeEvent.latch;
        } else {
            writerStatistics.totalRowsCount.incrementAndGet();
            final RowChange rowChange = rowChangeEvent.rowChange;
            boolean succeed = buffer.appendRowChange(rowChange);
            if (!succeed) {
                request = buffer.makeRequest();
                buffer.clear();
                succeed = buffer.appendRowChange(rowChange);

                if (!succeed) {
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            writerStatistics.totalFailedRowsCount.incrementAndGet();
                            if (callback != null) {
                                callback.onFailed(rowChange, new ClientException("Can not even append only one row into buffer."));
                            }
                        }
                    });
                }
            }
        }

        if (request != null) {
            // send this request
            semaphore.acquire();
            logger.debug("Acquire semaphore, start send async request.");
            final BatchWriteRowRequest finalRequest = request;
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    writerStatistics.totalRequestCount.incrementAndGet();
                    ots.batchWriteRow(finalRequest, new FlushCallback(ots, new AtomicInteger(1), semaphore, callback, executor, writerStatistics));
                }
            });
        }

        if (shouldWaitFlush) {
            waitFlush();
            latch.countDown();
        }
    }

    private void waitFlush() throws InterruptedException {
        logger.debug("Wait flush.");
        for (int i = 0; i < concurrency; i++) {
            semaphore.acquire();
            logger.debug("Wait flush: {}, {}", i, concurrency);
        }
        semaphore.release(concurrency);
        logger.debug("Wait flush finished.");
    }
}
