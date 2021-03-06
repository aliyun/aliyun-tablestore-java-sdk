package com.alicloud.openservices.tablestore.model;

/**
 * 表示预定义列的数据类型，目前只支持{@link #INTEGER}、{@link #DOUBLE}、{@link #BOOLEAN}、{@link #STRING}和{@link #BINARY}这五种数据类型。
 */
public enum DefinedColumnType {
    /**
     * 64位整数。
     */
    INTEGER,

    /**
     * 浮点数。
     */
    DOUBLE,

    /**
     * 布尔值。
     */
    BOOLEAN,

    /**
     * 字符串。
     */
    STRING,

    /**
     * BINARY。
     */
    BINARY;
}
