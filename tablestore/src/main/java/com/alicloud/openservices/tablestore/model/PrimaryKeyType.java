package com.alicloud.openservices.tablestore.model;

/**
 * Indicates the data type of the primary key column. Currently, only three data types are supported: {@link #STRING}, {@link #INTEGER}, and {@link #BINARY}.
 */
public enum PrimaryKeyType {
    /**
     * String.
     */
    STRING,

    /**
     * 64-bit integer.
     */
    INTEGER,

    /**
     * Binary data.
     */
    BINARY;
}
