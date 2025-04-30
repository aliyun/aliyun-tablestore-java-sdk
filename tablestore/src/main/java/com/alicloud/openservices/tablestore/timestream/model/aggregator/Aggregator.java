package com.alicloud.openservices.tablestore.timestream.model.aggregator;

import com.alicloud.openservices.tablestore.model.ColumnValue;

public interface Aggregator<T> extends Cloneable {
    public String getName();
    public void add(T v);
    public ColumnValue getValue();
    public Aggregator clone();
}
