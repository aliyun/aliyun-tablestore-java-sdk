package com.alicloud.openservices.tablestore.timeserieswriter;

import com.alicloud.openservices.tablestore.AsyncTimeseriesClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.callback.TimeseriesRowResult;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesBucketConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.config.TimeseriesWriterConfig;
import com.alicloud.openservices.tablestore.timeserieswriter.group.TimeseriesGroup;
import com.alicloud.openservices.tablestore.timeserieswriter.handle.TimeseriesRowEventHandler;
import com.alicloud.openservices.tablestore.timeserieswriter.handle.TimeseriesWriterHandleStatistics;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class TimeseriesBucket {

    private Logger logger = LoggerFactory.getLogger(TimeseriesBucket.class);

    private final TimeseriesBucketConfig timeseriesBucketConfig;

    private final Disruptor<TimeseriesRowEvent> disruptor;

    private final RingBuffer<TimeseriesRowEvent> ringBuffer;

    private final TimeseriesRowEventHandler timeseriesRowEventHandler;
    private TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> resultCallback;

    public void setResultCallback(TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> resultCallback) {
        this.resultCallback = resultCallback;
        timeseriesRowEventHandler.setCallback(resultCallback);
    }

    private final ExecutorService disruptorExecutor;

    private final TimeseriesWriterConfig timeseriesWriterConfig;

    private final Semaphore semaphore;

    public TimeseriesBucket(TimeseriesBucketConfig timeseriesBucketConfig, AsyncTimeseriesClientInterface ots, TimeseriesWriterConfig timeseriesWriterConfig,
                            TableStoreCallback<TimeseriesTableRow, TimeseriesRowResult> resultCallback,
                            Executor executor,
                            TimeseriesWriterHandleStatistics timeseriesWriterHandleStatistics, Semaphore semaphore) {
        TimeseriesRowEvent.TimeseriesRowEventFactory factory = new TimeseriesRowEvent.TimeseriesRowEventFactory();
        this.timeseriesWriterConfig = timeseriesWriterConfig;
        this.timeseriesBucketConfig = timeseriesBucketConfig;
        this.semaphore = semaphore;
        this.resultCallback = resultCallback;
        disruptorExecutor = Executors.newFixedThreadPool(1);
        disruptor = new Disruptor<TimeseriesRowEvent>(factory, this.timeseriesWriterConfig.getBufferSize(), disruptorExecutor);
        ringBuffer = disruptor.getRingBuffer();
        timeseriesRowEventHandler = new TimeseriesRowEventHandler(ots, timeseriesWriterConfig, timeseriesBucketConfig,
                this.resultCallback,
                executor,
                timeseriesWriterHandleStatistics, this.semaphore);

        disruptor.handleEventsWith(timeseriesRowEventHandler);
        disruptor.start();


    }

    public boolean addRowChange(TimeseriesTableRow timeseriesTableRow, TimeseriesGroup timeseriesGroup) {
        try {
            long sequence = ringBuffer.tryNext();
            TimeseriesRowEvent timeseriesRowEvent = ringBuffer.get(sequence);
            timeseriesRowEvent.setValue(timeseriesTableRow, timeseriesGroup);
            ringBuffer.publish(sequence);

            return true;
        } catch (InsufficientCapacityException e) {
            return false;
        }
    }

    public void addSignal(CountDownLatch latch) {
        while (true) {
            try {
                long sequence = ringBuffer.tryNext();
                TimeseriesRowEvent timeseriesRowEvent = ringBuffer.get(sequence);
                timeseriesRowEvent.setValue(latch);
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
        return this.timeseriesBucketConfig.getBucketId();
    }

    public RingBuffer<TimeseriesRowEvent> getRingBuffer() {
        return ringBuffer;
    }

}
