package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.model.RowChange;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class WriterResult {
    private boolean isAllFinished = true;
    private final int totalCount;
    private final AtomicReferenceArray<WriterResult.RowChangeStatus> rowChangeStatusList;

    public WriterResult (int totalCount, AtomicReferenceArray<WriterResult.RowChangeStatus> rowChangeStatusList) {
        this.totalCount = totalCount;
        this.rowChangeStatusList = rowChangeStatusList;
    }

    public List<RowChangeStatus> getSucceedRows() {
        List<RowChangeStatus> succeed = new LinkedList<RowChangeStatus>();
        for (int i = 0; i < rowChangeStatusList.length(); i++) {
            RowChangeStatus rowChangeStatus = rowChangeStatusList.get(i);
            if (rowChangeStatus.isSucceed()) {
                succeed.add(rowChangeStatus);
            }
        }
        return succeed;
    }

    public List<RowChangeStatus> getFailedRows() {
        List<RowChangeStatus> failed = new LinkedList<RowChangeStatus>();
        for (int i = 0; i < rowChangeStatusList.length(); i++) {
            RowChangeStatus rowChangeStatus = rowChangeStatusList.get(i);
            if (!rowChangeStatus.isSucceed()) {
                failed.add(rowChangeStatus);
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
        List<RowChangeStatus> failed = new LinkedList<RowChangeStatus>();
        for (int i = 0; i < rowChangeStatusList.length(); i++) {
            RowChangeStatus rowChangeStatus = rowChangeStatusList.get(i);
            if (!rowChangeStatus.isSucceed()) {
                failed.add(rowChangeStatus);
            }
        }
        return failed.size() == 0;
    }

    public static class RowChangeStatus {
        private boolean succeed;
        private Exception exception;
        private RowChange rowChange;

        public RowChangeStatus(boolean succeed, RowChange rowChange, Exception exception) {
            this.succeed = succeed;
            this.rowChange = rowChange;
            this.exception = exception;
        }

        public RowChange getRowChange() {
            return rowChange;
        }

        public boolean isSucceed() {
            return succeed;
        }

        public Exception getException() {
            return exception;
        }
    }
}
