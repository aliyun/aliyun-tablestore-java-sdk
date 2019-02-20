package com.alicloud.openservices.tablestore.model;

public enum Direction {
    /**
     * 正序读.
     */
    FORWARD,
    /**
     * 反序读.
     * 反序读时需要注意{@link RangeRowQueryCriteria#inclusiveStartPrimaryKey}要大于{@link RangeRowQueryCriteria#exclusiveEndPrimaryKey}.
     */
    BACKWARD
}
