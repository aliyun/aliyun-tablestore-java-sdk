package com.alicloud.openservices.tablestore.model.search;

/**
 * Field types supported in SearchIndex
 */
public enum FieldType {
    LONG,
    DOUBLE,
    BOOLEAN,
    /**
     * String type, the difference from Text is that keyword is not split into tokens and usually serves as a whole. If you want to perform aggregation and statistical analysis, please use this type.
     */
    KEYWORD,
    /**
     * String type, the difference from keyword is that text will be tokenized, generally used in fuzzy query scenarios.
     */
    TEXT,
    NESTED,
    GEO_POINT,
    DATE,
    VECTOR,
    FUZZY_KEYWORD,
    IP,
    JSON,
    /**
     * Unknown type, please upgrade to the latest SDK version if you encounter this type.
     */
    UNKNOWN,
}
