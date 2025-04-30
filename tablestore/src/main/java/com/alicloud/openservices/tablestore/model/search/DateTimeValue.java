package com.alicloud.openservices.tablestore.model.search;


public class DateTimeValue {
    private Integer value;
    private DateTimeUnit unit;

    public DateTimeValue() {
    }

    public DateTimeValue(Integer value, DateTimeUnit unit) {
        this.value = value;
        this.unit = unit;
    }

    public Integer getValue() {
        return value;
    }

    public DateTimeValue setValue(Integer value) {
        this.value = value;
        return this;
    }

    public DateTimeUnit getUnit() {
        return unit;
    }

    public DateTimeValue setUnit(DateTimeUnit unit) {
        this.unit = unit;
        return this;
    }
}
