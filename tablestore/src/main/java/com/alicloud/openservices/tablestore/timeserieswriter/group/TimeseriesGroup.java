package com.alicloud.openservices.tablestore.timeserieswriter.group;

import com.alicloud.openservices.tablestore.core.CallbackImpledFuture;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;
import com.alicloud.openservices.tablestore.timeserieswriter.TimeseriesWriterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TimeseriesGroup {
    private Logger logger = LoggerFactory.getLogger(TimeseriesGroup.class);

    private static AtomicLong idGenerator = new AtomicLong(0);
    private final long groupId;
    private final int totalCount;
    private final AtomicInteger remainCounter;
    private final AtomicReferenceArray<TimeseriesWriterResult.TimeseriesRowChangeStatus> rowChangeStatusList;
    private final CallbackImpledFuture<TimeseriesTableRow, TimeseriesWriterResult> future;

    public TimeseriesGroup(int totalCount) {
        this.future = new CallbackImpledFuture<TimeseriesTableRow, TimeseriesWriterResult>();
        this.totalCount = totalCount;
        this.remainCounter = new AtomicInteger(totalCount);
        this.rowChangeStatusList = new AtomicReferenceArray<TimeseriesWriterResult.TimeseriesRowChangeStatus>(totalCount);
        this.groupId = idGenerator.incrementAndGet();
    }

    public CallbackImpledFuture<TimeseriesTableRow, TimeseriesWriterResult> getFuture() {
        return future;
    }

    public void succeedOneRow(TimeseriesTableRow timeseriesTableRow) {
        finishOneRow(true, timeseriesTableRow, null);
    }

    public void failedOneRow(TimeseriesTableRow timeseriesTableRow, Exception exception) {
        finishOneRow(false, timeseriesTableRow, exception);
    }

    private void finishOneRow(boolean isSucceed, TimeseriesTableRow timeseriesTableRow, Exception exception) {
        int counter = this.remainCounter.decrementAndGet();

        if (counter < 0) {
            RuntimeException exp = new IllegalStateException(
                    String.format("[%d] WriterResult shouldn't finish more rows than total count", groupId));
            logger.error("Group OnFinishOneRow Failed", exp);
            throw exp;
        }

        TimeseriesWriterResult.TimeseriesRowChangeStatus status = new TimeseriesWriterResult.TimeseriesRowChangeStatus(isSucceed, timeseriesTableRow, exception);
        rowChangeStatusList.set(totalCount - counter - 1, status); // Complete row offset = totalCount - counter - 1

        if (counter == 0) {
            completeGroup();
        }
    }

    private void completeGroup() {
        TimeseriesWriterResult timeseriesWriterResult = new TimeseriesWriterResult(totalCount, rowChangeStatusList);
        future.onCompleted(null, timeseriesWriterResult);
    }

    public long getGroupId() {
        return groupId;
    }
}
