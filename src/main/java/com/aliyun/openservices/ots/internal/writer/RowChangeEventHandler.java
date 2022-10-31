package com.aliyun.openservices.ots.internal.writer;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSAsync;
import com.aliyun.openservices.ots.OTSException;
import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.model.*;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RowChangeEventHandler implements EventHandler<RowChangeEvent> {
    private Logger logger = LoggerFactory.getLogger(RowChangeEventHandler.class);

    private OTSAsync ots;
    private WriteRPCBuffer buffer;
    private int concurrency;
    private Semaphore semaphore;
    private OTSCallback<RowChange, ConsumedCapacity> callback;
    private Executor executor;
    private AtomicLong totalRequestCount;

    public RowChangeEventHandler(OTSAsync ots, WriterConfig writerConfig, OTSCallback<RowChange, ConsumedCapacity> callback, Executor executor) {
        this.ots = ots;
        this.buffer = new WriteRPCBuffer(writerConfig);
        this.concurrency = writerConfig.getConcurrency();
        this.semaphore = new Semaphore(concurrency);
        this.callback = callback;
        this.executor = executor;
        this.totalRequestCount = new AtomicLong();
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
                            callback.onFailed(new OTSContext<RowChange, ConsumedCapacity>(rowChange, null),
                                    new ClientException("Can not even append only one row into buffer."));
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
                    totalRequestCount.incrementAndGet();
                    ots.batchWriteRow(finalRequest, new FlushCallback(ots, new AtomicInteger(1), semaphore, callback, executor));
                }
            });
            //ots.batchWriteRow(finalRequest, new FlushCallback<BatchWriteRowRequest, BatchWriteRowResult>(ots, new AtomicInteger(1), semaphore, callback, executor));
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

    public long getTotalRPCCount() {
        return totalRequestCount.get();
    }
}
