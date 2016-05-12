package com.aliyun.openservices.ots;

import com.aliyun.openservices.ots.internal.OTSCallback;
import com.aliyun.openservices.ots.internal.writer.RowChangeEvent;
import com.aliyun.openservices.ots.internal.writer.RowChangeEventHandler;
import com.aliyun.openservices.ots.internal.writer.WriterConfig;
import com.aliyun.openservices.ots.model.*;
import com.aliyun.openservices.ots.utils.CalculateHelper;
import com.aliyun.openservices.ots.utils.ParamChecker;
import com.aliyun.openservices.ots.utils.Preconditions;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultOTSWriter implements OTSWriter {
    private Logger logger = LoggerFactory.getLogger(OTSWriter.class);

    private OTSAsync ots;

    private Executor executor;

    private WriterConfig writerConfig;

    private OTSCallback<RowChange, ConsumedCapacity> callback;

    private String tableName;

    private TableMeta tableMeta;

    private Timer flushTimer;

    private ReentrantLock lock;

    private Disruptor<RowChangeEvent> disruptor;

    private RingBuffer<RowChangeEvent> ringBuffer;

    private RowChangeEventHandler eventHandler;

    public DefaultOTSWriter(OTSAsync ots, String tableName, WriterConfig config, OTSCallback<RowChange, ConsumedCapacity> callback, Executor executor) {
        Preconditions.checkNotNull(ots, "The ots client can not be null.");
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The table name can not be null or empty.");
        Preconditions.checkNotNull(executor, "The executor service can not be null.");
        this.ots = ots;
        this.tableName = tableName;
        this.writerConfig = config;
        this.callback = callback;
        this.executor = executor;
        flushTimer = new Timer();
        lock = new ReentrantLock();

        initialize();
    }

    private void initialize() {
        logger.info("Start initialize ots writer, table name: {}.", tableName);
        DescribeTableRequest request = new DescribeTableRequest();
        request.setTableName(tableName);
        OTSFuture<DescribeTableResult> result = ots.describeTable(request);
        DescribeTableResult res = result.get();
        this.tableMeta = res.getTableMeta();
        logger.info("End initialize with table meta: {}.", tableMeta);

        RowChangeEvent.RowChangeEventFactory factory = new RowChangeEvent.RowChangeEventFactory();

        // start flush thread, we only need one event handler, so we just set a thread pool with fixed size 1.
        disruptor = new Disruptor<RowChangeEvent>(factory, writerConfig.getBufferSize(), Executors.newFixedThreadPool(1));
        ringBuffer = disruptor.getRingBuffer();
        eventHandler = new RowChangeEventHandler(ots, writerConfig, callback, executor);
        disruptor.handleEventsWith(eventHandler);
        disruptor.start();

        // start flush timer
        startFlushTimer(writerConfig.getFlushInterval());
    }

    public long getTotalRPCCount() {
        return eventHandler.getTotalRPCCount();
    }

    public void startFlushTimer(int flushInterval) {

        this.flushTimer.cancel();

        this.flushTimer = new Timer();
        this.flushTimer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                triggerFlush();
            }
        }, flushInterval, flushInterval);
    }

    @Override
    public void addRowChange(RowChange rowChange) {
        ParamChecker.checkRowChange(tableMeta, rowChange, writerConfig);
        while (true) {
            try {
                long sequence = ringBuffer.tryNext();
                RowChangeEvent event = ringBuffer.get(sequence);
                event.setValue(rowChange);
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

    private void addSignal(ReentrantLock lock, Condition condition) {
        while (true) {
            try {
                long sequence = ringBuffer.tryNext();
                RowChangeEvent event = ringBuffer.get(sequence);
                event.setValue(lock, condition);
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
    public void setCallback(OTSCallback<RowChange, ConsumedCapacity> callback) {
        this.callback = callback;
    }

    @Override
    public OTSCallback<RowChange, ConsumedCapacity> getCallback() {
        return this.callback;
    }


    @Override
    public WriterConfig getWriterConfig() {
        return writerConfig;
    }

    private void triggerFlush() {
        Condition cond = lock.newCondition();
        addSignal(lock, cond);
    }

    @Override
    public synchronized void flush() throws ClientException {
        logger.debug("trigger flush and waiting.");
        Condition cond = lock.newCondition();
        lock.lock();
        try {
            addSignal(lock, cond);
            cond.await();
        } catch (InterruptedException e) {
            throw new ClientException(e);
        } finally {
            lock.unlock();
        }
        logger.debug("user trigger flush finished.");
    }

    @Override
    public synchronized void close() {
        flushTimer.cancel();
        flush();
        disruptor.shutdown();
    }
}
