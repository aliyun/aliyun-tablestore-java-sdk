package com.alicloud.openservices.tablestore.model;

public enum Priority {
    LOW(0),
    NORMAL(1),
    HIGH(2);
    private final long value;

    Priority(long value) {this.value = value;}

    public long getValue() {
        return value;
    }
}
