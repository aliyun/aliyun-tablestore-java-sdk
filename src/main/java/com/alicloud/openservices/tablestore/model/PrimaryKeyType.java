package com.alicloud.openservices.tablestore.model;

/**
 * 表示主键列的数据类型，目前只支持{@link #STRING}、{@link #INTEGER}、{@link #BINARY}和{@link #DATETIME}这三种数据类型。
 */
public enum PrimaryKeyType {
    /**
     * 字符串。
     */
    STRING,

    /**
     * 64位整数。
     */
    INTEGER,

    /**
     * 二进制数据。
     */
    BINARY,

    /**
     * 日期数据。
     */
    DATETIME;

}
