package com.alicloud.openservices.tablestore.model.search.agg;

import com.alicloud.openservices.tablestore.model.ColumnValue;

public class PercentilesAggregationItem {

    private double key;
    private ColumnValue value;

    public double getKey() {
        return key;
    }

    public PercentilesAggregationItem setKey(double key) {
        this.key = key;
        return this;
    }

    public ColumnValue getValue() {
        return value;
    }

    public PercentilesAggregationItem setValue(ColumnValue value) {
        this.value = value;
        return this;
    }
}
