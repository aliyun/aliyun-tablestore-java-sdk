package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.writer.config.BucketConfig;
import com.alicloud.openservices.tablestore.writer.handle.RowEventHandler;
import com.alicloud.openservices.tablestore.writer.handle.WriterHandleStatistics;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;


public class Bucket {
    private static Logger logger = LoggerFactory.getLogger(Bucket.class);

    private final BucketConfig bucketConfig;

    private final Disruptor<RowChangeEvent> disruptor;

    private final RingBuffer<RowChangeEvent> ringBuffer;

    private final RowEventHandler eventHandler;

    private final ExecutorService disruptorExecutor;

    private final WriterConfig writerConfig;

    private final Semaphore semaphore;

    private TableStoreCallback<RowChange, RowWriteResult> resultCallback;

    public Bucket(BucketConfig bucketConfig, AsyncClientInterface ots, WriterConfig writerConfig,
                  TableStoreCallback<RowChange, RowWriteResult> resultCallback, Executor executor,
                  WriterHandleStatistics writerStatistics, Semaphore semaphore) {
        RowChangeEvent.RowChangeEventFactory factory = new RowChangeEvent.RowChangeEventFactory();

        this.bucketConfig = bucketConfig;
        this.writerConfig = writerConfig;
        this.semaphore = semaphore;
        this.resultCallback = resultCallback;
        disruptorExecutor = Executors.newFixedThreadPool(1);
        disruptor = new Disruptor<RowChangeEvent>(factory, this.writerConfig.getBufferSize(), disruptorExecutor);
        ringBuffer = disruptor.getRingBuffer();
        eventHandler = new RowEventHandler(ots, bucketConfig, writerConfig, this.resultCallback, executor,
                writerStatistics, this.semaphore);

        disruptor.handleEventsWith(eventHandler);
        disruptor.start();
    }


    public boolean addRowChange(RowChange rowChange, Group group) {
        try {
            long sequence = ringBuffer.tryNext();
            RowChangeEvent event = ringBuffer.get(sequence);
            event.setValue(rowChange, group);
            ringBuffer.publish(sequence);

            return true;
        } catch (InsufficientCapacityException e) {
            return false;
        }
    }

    public void setResultCallback(TableStoreCallback<RowChange, RowWriteResult> resultCallback) {
        eventHandler.setCallback(resultCallback);
    }


    public void addSignal(CountDownLatch latch) {
        while (true) {
            try {
                long sequence = ringBuffer.tryNext();
                RowChangeEvent event = ringBuffer.get(sequence);
                event.setValue(latch);
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

    public synchronized void close() {
        disruptor.shutdown();
        disruptorExecutor.shutdown();
    }

    public int getId() {
        return this.bucketConfig.getBucketId();
    }

    public RingBuffer<RowChangeEvent> getRingBuffer() {
        return ringBuffer;
    }
}
