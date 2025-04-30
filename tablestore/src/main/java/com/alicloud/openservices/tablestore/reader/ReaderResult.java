package com.alicloud.openservices.tablestore.reader;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class ReaderResult {
    private final int totalCount;
    private final AtomicReferenceArray<RowReadResult> resultList;
    private boolean isAllFinished = true;

    public ReaderResult(int totalCount, AtomicReferenceArray<RowReadResult> resultList) {
        this.totalCount = totalCount;
        this.resultList = resultList;
    }

    public List<RowReadResult> getSucceedRows() {
        List<RowReadResult> succeed = new LinkedList<RowReadResult>();
        for (int i = 0; i < resultList.length(); i++) {
            RowReadResult result = resultList.get(i);
            if (result.isSucceed()) {
                succeed.add(result);
            }
        }
        return succeed;
    }

    public List<RowReadResult> getFailedRows() {
        List<RowReadResult> failed = new LinkedList<RowReadResult>();
        for (int i = 0; i < resultList.length(); i++) {
            RowReadResult result = resultList.get(i);
            if (!result.isSucceed()) {
                failed.add(result);
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
        List<RowReadResult> failed = new LinkedList<RowReadResult>();
        for (int i = 0; i < resultList.length(); i++) {
            RowReadResult result = resultList.get(i);
            if (!result.isSucceed()) {
                return false;
            }
        }
        return true;
    }

}
