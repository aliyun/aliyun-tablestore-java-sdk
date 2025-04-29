package com.alicloud.openservices.tablestore.writer.handle;

import com.alicloud.openservices.tablestore.writer.WriterStatistics;

import java.util.concurrent.atomic.AtomicLong;

public class WriterHandleStatistics implements WriterStatistics {
    AtomicLong totalRequestCount = new AtomicLong(0);
    AtomicLong totalRowsCount = new AtomicLong(0);
    AtomicLong totalSucceedRowsCount = new AtomicLong(0);
    AtomicLong totalFailedRowsCount = new AtomicLong(0);
    AtomicLong totalSingleRowRequestCount = new AtomicLong(0);

    public WriterHandleStatistics() {

    }

    @Override
    public long getTotalRequestCount() {
        return totalRequestCount.longValue();
    }

    @Override
    public long getTotalRowsCount() {
        return totalRowsCount.longValue();
    }

    @Override
    public long getTotalSucceedRowsCount() {
        return totalSucceedRowsCount.longValue();
    }

    @Override
    public long getTotalFailedRowsCount() {
        return totalFailedRowsCount.longValue();
    }

    @Override
    public long getTotalSingleRowRequestCount() {
        return totalSingleRowRequestCount.longValue();
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("WriterStatistics: {\n")
                .append("    totalRequestCount=").append(totalRequestCount.get()).append(",\n")
                .append("    totalRowsCount=").append(totalRowsCount.get()).append(",\n")
                .append("    totalSucceedRowsCount=").append(totalSucceedRowsCount.get()).append(",\n")
                .append("    totalFailedRowsCount=").append(totalFailedRowsCount.get()).append(",\n")
                .append("    totalSingleRowRequestCount=").append(totalSingleRowRequestCount.get()).append(",\n")
                .append("}");

        return builder.toString();
    }
}
