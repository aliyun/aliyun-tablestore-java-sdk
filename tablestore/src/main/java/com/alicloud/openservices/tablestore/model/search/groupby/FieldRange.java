package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.model.ColumnValue;

public class FieldRange {

    private ColumnValue min;

    private ColumnValue max;

    public ColumnValue getMin() {
        return min;
    }

    public void setMin(ColumnValue min) {
        this.min = min;
    }

    public ColumnValue getMax() {
        return max;
    }

    public void setMax(ColumnValue max) {
        this.max = max;
    }

    public FieldRange(ColumnValue min, ColumnValue max) {
        this.min = min;
        this.max = max;
    }
}
