package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.core.utils.ParamChecker;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.writer.*;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultTableStoreWriter implements TableStoreWriter {
    private Logger logger = LoggerFactory.getLogger(TableStoreWriter.class);

    private AsyncClientInterface ots;

    private Executor executor;

    private WriterConfig writerConfig;

    private TableStoreCallback<RowChange, ConsumedCapacity> callback;

    private TableStoreCallback<RowChange, RowWriteResult> resultCallback;

    private String tableName;

    private TableMeta tableMeta;

    private Timer flushTimer;

    private Disruptor<RowChangeEvent> disruptor;

    private RingBuffer<RowChangeEvent> ringBuffer;

    private RowChangeEventHandler eventHandler;

    private ExecutorService disruptorExecutor;

    private DefaultWriterStatistics writerStatistics;

    private AtomicBoolean closed = new AtomicBoolean(false);

    public DefaultTableStoreWriter(AsyncClientInterface ots, String tableName, WriterConfig config, TableStoreCallback<RowChange, ConsumedCapacity> callback, Executor executor) {
        Preconditions.checkNotNull(ots, "The ots client can not be null.");
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The table name can not be null or empty.");
        Preconditions.checkNotNull(executor, "The executor service can not be null.");
        this.writerStatistics = new DefaultWriterStatistics();
        this.ots = ots;
        this.tableName = tableName;
        this.writerConfig = config;
        this.callback = callback;
        this.resultCallback = createResultCallback(callback);
        this.executor = executor;
        flushTimer = new Timer();

        initialize();
        closed.set(false);
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
        this.tableMeta = res.getTableMeta();
        logger.info("End initialize with table meta: {}.", tableMeta);

        RowChangeEvent.RowChangeEventFactory factory = new RowChangeEvent.RowChangeEventFactory();

        // start flush thread, we only need one event handler, so we just set a thread pool with fixed size 1.
        disruptorExecutor = Executors.newFixedThreadPool(1);
        disruptor = new Disruptor<RowChangeEvent>(factory, writerConfig.getBufferSize(), disruptorExecutor);
        ringBuffer = disruptor.getRingBuffer();
        eventHandler = new RowChangeEventHandler(ots, writerConfig, resultCallback, executor, writerStatistics);
        disruptor.handleEventsWith(eventHandler);
        disruptor.start();

        // start flush timer
        startFlushTimer(writerConfig.getFlushInterval());
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
        if (writerConfig.isEnableSchemaCheck()) {
            ParamChecker.checkRowChange(tableMeta, rowChange, writerConfig);
        }

        while (true) {
            if (!addRowChangeInternal(rowChange)) {
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
    public boolean tryAddRowChange(RowChange rowChange) {
        if (writerConfig.isEnableSchemaCheck()) {
            ParamChecker.checkRowChange(tableMeta, rowChange, writerConfig);
        }

        return addRowChangeInternal(rowChange);
    }

    public boolean addRowChangeInternal(RowChange rowChange) {
        if (closed.get()) {
            throw new ClientException("The writer has been closed.");
        }

        try {
            long sequence = ringBuffer.tryNext();
            RowChangeEvent event = ringBuffer.get(sequence);
            event.setValue(rowChange);
            ringBuffer.publish(sequence);
            return true;
        } catch (InsufficientCapacityException e) {
            return false;
        }
    }

    private void addSignal(CountDownLatch latch) {
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
    public void setCallback(final TableStoreCallback<RowChange, ConsumedCapacity> callback) {
        this.callback = callback;
        this.resultCallback = createResultCallback(callback);
        eventHandler.setCallback(resultCallback);
    }

    @Override
    public void setResultCallback(TableStoreCallback<RowChange, RowWriteResult> resultCallback) {
        this.callback = null;
        this.resultCallback = resultCallback;
        eventHandler.setCallback(resultCallback);
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
        CountDownLatch latch = new CountDownLatch(1);
        addSignal(latch);
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

        flushTimer.cancel();
        flush();
        disruptor.shutdown();
        disruptorExecutor.shutdown();

        closed.set(true);
    }
}
