package com.alicloud.openservices.tablestore.reader;

import java.util.concurrent.atomic.AtomicLong;

public class ReaderStatistics {
    AtomicLong totalRequestCount = new AtomicLong(0);
    AtomicLong totalRowsCount = new AtomicLong(0);
    AtomicLong totalSucceedRowsCount = new AtomicLong(0);
    AtomicLong totalFailedRowsCount = new AtomicLong(0);
    AtomicLong totalSingleRowRequestCount = new AtomicLong(0);

    public ReaderStatistics() {

    }

    public long getTotalRequestCount() {
        return totalRequestCount.longValue();
    }

    public long getTotalRowsCount() {
        return totalRowsCount.longValue();
    }

    public long getTotalSucceedRowsCount() {
        return totalSucceedRowsCount.longValue();
    }

    public long getTotalFailedRowsCount() {
        return totalFailedRowsCount.longValue();
    }

    public long getTotalSingleRowRequestCount() {
        return totalSingleRowRequestCount.longValue();
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ReaderStatistics: {\n")
                .append("    totalRequestCount=").append(totalRequestCount.get()).append(",\n")
                .append("    totalRowsCount=").append(totalRowsCount.get()).append(",\n")
                .append("    totalSucceedRowsCount=").append(totalSucceedRowsCount.get()).append(",\n")
                .append("    totalFailedRowsCount=").append(totalFailedRowsCount.get()).append(",\n")
                .append("    totalSingleRowRequestCount=").append(totalSingleRowRequestCount.get()).append(",\n")
                .append("}");

        return builder.toString();
    }
}
