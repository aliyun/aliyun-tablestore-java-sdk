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
    BINARY;

}
