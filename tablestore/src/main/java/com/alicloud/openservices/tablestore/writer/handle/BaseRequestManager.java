package com.alicloud.openservices.tablestore.writer.handle;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.writer.*;
import com.alicloud.openservices.tablestore.writer.config.BucketConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;


/**
 * Request construction management
 *
 */
public abstract class BaseRequestManager implements RequestManager {
    private static Logger logger = LoggerFactory.getLogger(BaseRequestManager.class);

    protected AsyncClientInterface ots;
    protected BucketConfig bucketConfig;
    protected WriterConfig writerConfig;
    protected Semaphore callbackSemaphore;
    protected TableStoreCallback<RowChange, RowWriteResult> callback;
    protected Executor executor;
    protected WriterHandleStatistics writerStatistics;
    protected Semaphore bucketSemaphore;

    protected CallbackFactory callbackFactory;
    protected List<RowChangeWithGroup> rowChangeWithGroups = new LinkedList<RowChangeWithGroup>();

    protected int totalSize;
    protected int totalRowsCount;

    protected boolean allowDuplicatedRowInBatchRequest;
    protected ConcurrentSkipListSet<PrimaryKey> sendingPrimarykeys = new ConcurrentSkipListSet<PrimaryKey>();

    public BaseRequestManager(AsyncClientInterface ots, WriterConfig writerConfig, BucketConfig bucketConfig, Executor executor,
                                       WriterHandleStatistics writerStatistics, TableStoreCallback<RowChange, RowWriteResult> callback,
                                       Semaphore callbackSemaphore, Semaphore bucketSemaphore) {
        this.ots = ots;
        this.writerConfig = writerConfig;
        this.bucketConfig = bucketConfig;
        this.executor = executor;
        this.writerStatistics = writerStatistics;
        this.callback = callback;
        this.callbackSemaphore = callbackSemaphore;
        this.bucketSemaphore = bucketSemaphore;
        this.totalSize = 0;
        this.totalRowsCount = 0;
        this.allowDuplicatedRowInBatchRequest = bucketConfig.isAllowDuplicateRowInBatchRequest();
        this.callbackFactory =  new FlushCallbackFactory(ots, callbackSemaphore, callback, executor,
                writerStatistics, bucketConfig, bucketSemaphore);
    }

    @Override
    public boolean appendRowChange(RowChangeWithGroup rowChangeWithGroup) {
        if (totalSize + rowChangeWithGroup.rowChange.getDataSize() > writerConfig.getMaxBatchSize()) {
            return false;
        }

        if (totalRowsCount >= writerConfig.getMaxBatchRowsCount()) {
            return false;
        }

        if (!allowDuplicatedRowInBatchRequest) {
            if (sendingPrimarykeys.contains(rowChangeWithGroup.rowChange.getPrimaryKey())) {
                return false;
            } else {
                sendingPrimarykeys.add(rowChangeWithGroup.rowChange.getPrimaryKey());
            }
        }

        rowChangeWithGroups.add(rowChangeWithGroup);
        this.totalSize += rowChangeWithGroup.rowChange.getDataSize();
        this.totalRowsCount += 1;
        return true;
    }

    @Override
    public int getTotalRowsCount() {
        return totalRowsCount;
    }

    @Override
    public void clear() {
        rowChangeWithGroups.clear();
        sendingPrimarykeys.clear();
        totalSize = 0;
        totalRowsCount = 0;
    }
}
