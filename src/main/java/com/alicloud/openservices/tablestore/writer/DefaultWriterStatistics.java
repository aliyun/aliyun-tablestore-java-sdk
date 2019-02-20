package com.alicloud.openservices.tablestore.writer;

import java.util.concurrent.atomic.AtomicLong;

public class DefaultWriterStatistics implements WriterStatistics {
    AtomicLong totalRequestCount = new AtomicLong(0);
    AtomicLong totalRowsCount = new AtomicLong(0);
    AtomicLong totalSucceedRowsCount = new AtomicLong(0);
    AtomicLong totalFailedRowsCount = new AtomicLong(0);
    AtomicLong totalSingleRowRequestCount = new AtomicLong(0);

    public DefaultWriterStatistics() {

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
}
