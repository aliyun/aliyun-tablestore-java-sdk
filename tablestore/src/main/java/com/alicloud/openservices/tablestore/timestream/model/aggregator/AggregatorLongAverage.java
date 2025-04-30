package com.alicloud.openservices.tablestore.timestream.model.aggregator;

import com.alicloud.openservices.tablestore.model.ColumnValue;

public class AggregatorLongAverage implements Aggregator<Long> {
    private String name;
    private long sum = 0;
    private long count = 0;

    public AggregatorLongAverage(String name) {
        this(name, 0, 0);
    }

    public AggregatorLongAverage(String name, long sum, long count) {
        this.name = name;
        this.sum = sum;
        this.count = count;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void add(Long v) {
        sum += v;
    }

    @Override
    public ColumnValue getValue() {
        if (count == 0) {
            return ColumnValue.fromDouble(0);
        }
        return ColumnValue.fromDouble(sum / count);
    }

    @Override
    public Aggregator clone() {
        return new AggregatorLongAverage(name, sum, count);
    }
}
