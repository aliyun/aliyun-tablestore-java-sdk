package com.alicloud.openservices.tablestore.writer;

public interface WriterStatistics {
    long getTotalRequestCount();
    long getTotalRowsCount();
    long getTotalSucceedRowsCount();
    long getTotalFailedRowsCount();
    long getTotalSingleRowRequestCount();
}
