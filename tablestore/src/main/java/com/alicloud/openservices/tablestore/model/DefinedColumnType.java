package com.alicloud.openservices.tablestore.model;

/**
 * Indicates the data type of predefined columns. Currently, only five data types are supported: {@link #INTEGER}, {@link #DOUBLE}, {@link #BOOLEAN}, {@link #STRING}, and {@link #BINARY}.
 */
public enum DefinedColumnType {
    /**
     * 64-bit integer.
     */
    INTEGER,

    /**
     * Floating point number.
     */
    DOUBLE,

    /**
     * Boolean value.
     */
    BOOLEAN,

    /**
     * String.
     */
    STRING,

    /**
     * BINARY.
     */
    BINARY;
}
