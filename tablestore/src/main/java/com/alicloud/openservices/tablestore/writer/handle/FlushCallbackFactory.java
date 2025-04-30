package com.alicloud.openservices.tablestore.writer.handle;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.DeleteRowResponse;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.writer.CallbackFactory;
import com.alicloud.openservices.tablestore.writer.Group;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.config.BucketConfig;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;


public class FlushCallbackFactory implements CallbackFactory {
    protected AsyncClientInterface ots;
    protected BucketConfig bucketConfig;
    protected WriterConfig writerConfig;
    protected Semaphore callbackSemaphore;
    protected TableStoreCallback<RowChange, RowWriteResult> callback;
    protected Executor executor;
    protected WriterHandleStatistics writerStatistics;
    protected Semaphore bucketSemaphore;

    public FlushCallbackFactory(AsyncClientInterface ots, Semaphore callbackSemaphore,
                                TableStoreCallback<RowChange, RowWriteResult> callback,
                                Executor executor, WriterHandleStatistics writerStatistics,
                                BucketConfig bucketConfig, Semaphore bucketSemaphore) {
        this.ots = ots;
        this.callbackSemaphore = callbackSemaphore;
        this.bucketConfig = bucketConfig;
        this.callback = callback;
        this.executor = executor;
        this.writerStatistics = writerStatistics;
        this.bucketSemaphore = bucketSemaphore;
    }

    @Override
    public TableStoreCallback newInstance(List<Group> groupList) {
        return new FlushCallback<DeleteRowRequest, DeleteRowResponse>(
                ots, new AtomicInteger(1), callbackSemaphore, callback, executor,
                writerStatistics, bucketConfig, bucketSemaphore, groupList);
    }
}
