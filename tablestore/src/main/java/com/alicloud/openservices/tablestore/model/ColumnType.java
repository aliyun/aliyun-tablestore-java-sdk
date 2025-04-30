package com.alicloud.openservices.tablestore.model;

/**
 * Indicates the data types of attribute columns. Currently, only five data types are supported: {@link #STRING}, {@link #INTEGER}, {@link #BINARY}, {@link #DOUBLE}, and {@link #BOOLEAN}.
 */
public enum ColumnType {
    /**
     * String type.
     */
    STRING,

    /**
     * 64-bit signed integer.
     */
    INTEGER,

    /**
     * Boolean type.
     */
    BOOLEAN,

    /**
     * 64-bit double-precision floating-point type.
     */
    DOUBLE,
    
    /**
     * Binary data.
     */
    BINARY,

    /**
     * Time type, formatted as 'YYYY-MM-DD hh:mm:ss.xxxxxx', represented by the java.sql.Timestamp type.
     */
    DATETIME,

    /**
     * Time interval type, formatted as 'hh:mm:ss.xxxxxx', represented using the java.sql.Time type.
     */
    TIME,

    /**
     * Date type, formatted as 'YYYY-MM-DD', represented by the java.sql.Date type.
     */
    DATE,
}
