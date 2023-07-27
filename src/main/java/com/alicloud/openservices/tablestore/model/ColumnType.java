package com.alicloud.openservices.tablestore.model;

/**
 * 表示属性列的数据类型，目前只支持{@link #STRING}、{@link #INTEGER}、{@link #BINARY}、{@link #DOUBLE}和{@link #BOOLEAN}这五种数据类型。
 */
public enum ColumnType {
    /**
     * 字符串型。
     */
    STRING,

    /**
     * 64位带符号的整型。
     */
    INTEGER,

    /**
     * 布尔型。
     */
    BOOLEAN,

    /**
     * 64位浮点型。
     */
    DOUBLE,
    
    /**
     * 二进制数据。
     */
    BINARY,

    /**
     * 时间类型，格式为‘YYYY-MM-DD hh:mm:ss.xxxxxx’，使用 java.sql.Timestamp类型表示。
     */
    DATETIME,

    /**
     * 时间间隔类型，格式为‘hh:mm:ss.xxxxxx’，使用 java.sql.Time类型表示。
     */
    TIME,

    /**
     * 日期类型，格式为‘YYYY-MM-DD’，使用 java.sql.Date类型表示。
     */
    DATE,
}
