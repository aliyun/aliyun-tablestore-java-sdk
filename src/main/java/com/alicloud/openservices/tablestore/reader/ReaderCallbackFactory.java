package com.alicloud.openservices.tablestore.reader;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;

public class ReaderCallbackFactory {
    private final AsyncClientInterface ots;
    private final Semaphore callbackSemaphore;
    private final TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback;
    private final Executor executor;
    private final Semaphore bucketSemaphore;
    private final ReaderStatistics statistics;
    private TableStoreReaderConfig config;

    public ReaderCallbackFactory(
            AsyncClientInterface ots,
            Semaphore callbackSemaphore,
            TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback,
            Executor executor,
            Semaphore bucketSemaphore,
            ReaderStatistics statistics) {
        this.ots = ots;
        this.callbackSemaphore = callbackSemaphore;
        this.callback = callback;
        this.executor = executor;
        this.bucketSemaphore = bucketSemaphore;
        this.statistics = statistics;
    }

    public TableStoreCallback newInstance(Map<String, List<ReaderGroup>> groupMap) {
        return new ReaderCallback<BatchGetRowRequest, BatchGetRowResponse>(
                ots, new AtomicInteger(1), callbackSemaphore, callback, executor, bucketSemaphore, statistics, groupMap);
    }
}
