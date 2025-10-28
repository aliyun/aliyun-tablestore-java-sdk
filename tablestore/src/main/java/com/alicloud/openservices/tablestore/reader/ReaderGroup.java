package com.alicloud.openservices.tablestore.reader;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import com.alicloud.openservices.tablestore.core.CallbackImpledFuture;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderGroup {
    private static AtomicLong idGenerator = new AtomicLong(0);
    private final long groupId;
    private final int totalCount;
    private final AtomicInteger remainCounter;
    private final AtomicReferenceArray<RowReadResult> resultList;
    private final CallbackImpledFuture<PrimaryKeyWithTable, ReaderResult> future;
    private static Logger logger = LoggerFactory.getLogger(ReaderGroup.class);

    public ReaderGroup(int totalCount) {
        this.future = new CallbackImpledFuture<PrimaryKeyWithTable, ReaderResult>();
        this.totalCount = totalCount;
        this.remainCounter = new AtomicInteger(totalCount);
        this.resultList = new AtomicReferenceArray<RowReadResult>(totalCount);
        this.groupId = idGenerator.incrementAndGet();
    }

    public CallbackImpledFuture<PrimaryKeyWithTable, ReaderResult> getFuture() {
        return future;
    }

    public void succeedOneRow(PrimaryKey primaryKey, BatchGetRowResponse.RowResult rowResult) {
        finishOneRow(true, primaryKey, rowResult, null);
    }

    public void failedOneRow(PrimaryKey primaryKey, BatchGetRowResponse.RowResult rowResult, Exception exception) {
        finishOneRow(false, primaryKey, rowResult, exception);
    }

    private void finishOneRow(boolean isSucceed, PrimaryKey primaryKey, BatchGetRowResponse.RowResult rowResult, Exception exception) {
        int counter = this.remainCounter.decrementAndGet();

        if (counter < 0) {
            RuntimeException exp = new IllegalStateException(
                    String.format("[%d] ReaderResult shouldn't finish more rows than total count", groupId));
            logger.error("Group OnFinishOneRow Failed", exp);
            throw exp;
        }

        RowReadResult rowReadResult = new RowReadResult(primaryKey, rowResult);
        resultList.set(totalCount - counter - 1, rowReadResult); // Complete row offset = totalCount - counter - 1
        if (counter == 0) {
            completeGroup();
        }
    }

    private void completeGroup() {
        ReaderResult readerResult = new ReaderResult(totalCount, resultList);
        future.onCompleted(null, readerResult);
    }

    public long getGroupId() {
        return groupId;
    }
}
