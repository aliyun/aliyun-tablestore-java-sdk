package com.alicloud.openservices.tablestore.timestream.model.aggregator;

import com.alicloud.openservices.tablestore.model.ColumnValue;

public class AggregatorLongSum implements Aggregator<Long> {
    private String name;
    private long sum = 0;
    private long count = 0;

    public AggregatorLongSum(String name) {
        this(name, 0, 0);
    }

    public AggregatorLongSum(String name, long sum, long count) {
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
        count++;
    }

    @Override
    public ColumnValue getValue() {
        return ColumnValue.fromLong(sum);
    }

    @Override
    public Aggregator clone() {
        return new AggregatorLongSum(name, sum, count);
    }
}
