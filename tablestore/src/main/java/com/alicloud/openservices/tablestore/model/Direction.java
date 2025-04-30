package com.alicloud.openservices.tablestore.model;

public enum Direction {
    /**
     * Read in ascending order.
     */
    FORWARD,
    /**
     * Reverse order read.
     * When reading in reverse order, note that {@link RangeRowQueryCriteria#inclusiveStartPrimaryKey} should be greater than {@link RangeRowQueryCriteria#exclusiveEndPrimaryKey}.
     */
    BACKWARD
}
