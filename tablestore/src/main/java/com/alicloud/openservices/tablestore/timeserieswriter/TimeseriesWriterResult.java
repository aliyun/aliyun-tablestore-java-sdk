package com.alicloud.openservices.tablestore.timeserieswriter;

import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesTableRow;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class TimeseriesWriterResult {
    private boolean isAllFinished = true;
    private final int totalCount;
    private final AtomicReferenceArray<TimeseriesWriterResult.TimeseriesRowChangeStatus> timeseriesRowChangeStatusList;

    public TimeseriesWriterResult(int totalCount, AtomicReferenceArray<TimeseriesWriterResult.TimeseriesRowChangeStatus> timeseriesRowChangeStatusList) {
        this.totalCount = totalCount;
        this.timeseriesRowChangeStatusList = timeseriesRowChangeStatusList;
    }

    public List<TimeseriesWriterResult.TimeseriesRowChangeStatus> getSucceedRows() {
        List<TimeseriesWriterResult.TimeseriesRowChangeStatus> succeed = new LinkedList<TimeseriesWriterResult.TimeseriesRowChangeStatus>();
        for (int i = 0; i < timeseriesRowChangeStatusList.length(); i++) {
            TimeseriesWriterResult.TimeseriesRowChangeStatus timeseriesRowChangeStatus = timeseriesRowChangeStatusList.get(i);
            if (timeseriesRowChangeStatus.isSucceed()) {
                succeed.add(timeseriesRowChangeStatus);
            }
        }
        return succeed;
    }

    public List<TimeseriesWriterResult.TimeseriesRowChangeStatus> getFailedRows() {
        List<TimeseriesWriterResult.TimeseriesRowChangeStatus> failed = new LinkedList<TimeseriesWriterResult.TimeseriesRowChangeStatus>();
        for (int i = 0; i < timeseriesRowChangeStatusList.length(); i++) {
            TimeseriesWriterResult.TimeseriesRowChangeStatus timeseriesRowChangeStatus = timeseriesRowChangeStatusList.get(i);
            if (!timeseriesRowChangeStatus.isSucceed()) {
                failed.add(timeseriesRowChangeStatus);
            }
        }
        return failed;
    }

    public int getTotalCount() {
        return totalCount;
    }

    public boolean isAllFinished() {
        return isAllFinished;
    }

    public boolean isAllSucceed() {
        List<TimeseriesWriterResult.TimeseriesRowChangeStatus> failed = new LinkedList<TimeseriesWriterResult.TimeseriesRowChangeStatus>();
        for (int i = 0; i < timeseriesRowChangeStatusList.length(); i++) {
            TimeseriesWriterResult.TimeseriesRowChangeStatus timeseriesRowChangeStatus = timeseriesRowChangeStatusList.get(i);
            if (!timeseriesRowChangeStatus.isSucceed()) {
                failed.add(timeseriesRowChangeStatus);
            }
        }
        return failed.size() == 0;
    }

    public static class TimeseriesRowChangeStatus {
        private boolean succeed;
        private Exception exception;
        private TimeseriesTableRow timeseriesTableRow;

        public TimeseriesRowChangeStatus(boolean succeed, TimeseriesTableRow timeseriesTableRow, Exception exception) {
            this.succeed = succeed;
            this.timeseriesTableRow = timeseriesTableRow;
            this.exception = exception;
        }

        public boolean isSucceed() {
            return succeed;
        }

        public Exception getException() {
            return exception;
        }

        public TimeseriesTableRow getTimeseriesTableRow() {
            return timeseriesTableRow;
        }
    }
}
