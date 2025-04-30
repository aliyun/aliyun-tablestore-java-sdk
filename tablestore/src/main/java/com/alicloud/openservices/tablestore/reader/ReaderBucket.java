package com.alicloud.openservices.tablestore.reader;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.RowQueryCriteria;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class ReaderBucket {
    private final Disruptor<ReaderEvent> disruptor;

    private final RingBuffer<ReaderEvent> ringBuffer;
    private final ReaderEventHandler readerEventHandler;
    private final ExecutorService disruptorExecutor;
    private final TableStoreReaderConfig config;
    private final TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback;

    public ReaderBucket(
            AsyncClientInterface ots,
            Semaphore semaphore,
            TableStoreReaderConfig config,
            TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback,
            Executor executor,
            ReaderStatistics statistics) {
        ReaderEvent.ReaderEventFactory factory = new ReaderEvent.ReaderEventFactory();

        this.callback = callback;
        this.disruptorExecutor = Executors.newFixedThreadPool(1);
        this.config = config;
        this.disruptor = new Disruptor<ReaderEvent>(factory, this.config.getBufferSize(), disruptorExecutor);
        this.ringBuffer = disruptor.getRingBuffer();
        this.readerEventHandler = new ReaderEventHandler(ots, config, executor, semaphore, callback, statistics);

        disruptor.handleEventsWith(readerEventHandler);
        disruptor.start();
    }

    public void setCallback(TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback) {
        readerEventHandler.setCallback(callback);
    }

    public boolean addPrimaryKeyWithTable(PrimaryKeyWithTable pkWithTable, final ReaderGroup readerGroup) {
        try {
            long sequence = ringBuffer.tryNext();
            ReaderEvent event = ringBuffer.get(sequence);
            event.setValue(pkWithTable, readerGroup);
            ringBuffer.publish(sequence);

            return true;
        } catch (InsufficientCapacityException e) {
            return false;
        }
    }

    public void addSignal(CountDownLatch latch, ReaderEvent.EventType type) {
        while (true) {
            try {
                long sequence = ringBuffer.tryNext();
                ReaderEvent event = ringBuffer.get(sequence);
                event.setValue(latch, type);
                ringBuffer.publish(sequence);

                return;
            } catch (InsufficientCapacityException e) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException exp) {
                }
            }
        }
    }

    public void setRowQueryCriteria(RowQueryCriteria rowQueryCriteria) {
        readerEventHandler.setRowQueryCriteria(rowQueryCriteria);
    }

    public RingBuffer<ReaderEvent> getRingBuffer() {
        return ringBuffer;
    }

    public synchronized void close() {
        disruptor.shutdown();
        disruptorExecutor.shutdown();
    }
}
