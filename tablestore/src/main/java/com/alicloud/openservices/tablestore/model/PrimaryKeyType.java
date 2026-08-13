package com.alicloud.openservices.tablestore.model;

/**
 * Indicates the data type of the primary key column. Currently supported types are:
 * {@link #STRING}, {@link #INTEGER}, {@link #BINARY}, and {@link #BOOLEAN}.
 * <p>Note: {@link #BOOLEAN} can only be used as a non-first primary key column.</p>
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
    BINARY,

    /**
     * Boolean. Can only be used as a non-first primary key column.
     */
    BOOLEAN;
}
