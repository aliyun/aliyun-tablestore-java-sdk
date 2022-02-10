package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.core.CallbackImpledFuture;
import com.alicloud.openservices.tablestore.model.RowChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;


public class Group {
    private Logger logger = LoggerFactory.getLogger(Group.class);

    private static AtomicLong idGenerator = new AtomicLong(0);
    private final long groupId;
    private final int totalCount;
    private final AtomicInteger remainCounter;
    private final AtomicReferenceArray<WriterResult.RowChangeStatus> rowChangeStatusList;
    private final CallbackImpledFuture<RowChange, WriterResult> future;

    public Group(int totalCount) {
        this.future = new CallbackImpledFuture<RowChange, WriterResult>();
        this.totalCount = totalCount;
        this.remainCounter = new AtomicInteger(totalCount);
        this.rowChangeStatusList = new AtomicReferenceArray<WriterResult.RowChangeStatus>(totalCount);
        this.groupId = idGenerator.incrementAndGet();
    }

    public CallbackImpledFuture<RowChange, WriterResult> getFuture() {
        return future;
    }

    public void succeedOneRow(RowChange rowChange) {
        finishOneRow(true, rowChange, null);
    }

    public void failedOneRow(RowChange rowChange, Exception exception) {
        finishOneRow(false, rowChange, exception);
    }

    private void finishOneRow(boolean isSucceed, RowChange rowChange, Exception exception) {
        int counter = this.remainCounter.decrementAndGet();

        if (counter < 0) {
            RuntimeException exp =  new IllegalStateException(
                    String.format("[%d] WriterResult shouldn't finish more rows than total count", groupId));
            logger.error("Group OnFinishOneRow Failed", exp);
            throw exp;
        }

        WriterResult.RowChangeStatus status = new WriterResult.RowChangeStatus(isSucceed, rowChange, exception);
        rowChangeStatusList.set(totalCount - counter - 1, status); // 完成行offset = totalCount - counter - 1

        if (counter == 0) {
            completeGroup();
        }
    }

    private void completeGroup() {
        WriterResult writerResult = new WriterResult(totalCount, rowChangeStatusList);
        future.onCompleted(null, writerResult);
    }

    public long getGroupId() {
        return groupId;
    }
}
