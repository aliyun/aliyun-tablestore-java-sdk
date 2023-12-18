package com.alicloud.openservices.tablestore.model.search;

/**
 * SearchIndex中支持的 Field 类型
 */
public enum FieldType {
    LONG,
    DOUBLE,
    BOOLEAN,
    /**
     * 字符串类型，同Text的区别是keyword不分词，一般作为一个整体。如果想要进行聚合统计分析，请使用该类型。
     */
    KEYWORD,
    /**
     * 字符串类型，同keyword的区别是text会进行分词，一般在模糊查询的场景使用。
     */
    TEXT,
    NESTED,
    GEO_POINT,
    DATE,
    VECTOR,
    /**
     * 未知的类型，遇到该类型请升级到最新SDK版本
     */
    UNKNOWN,
}
