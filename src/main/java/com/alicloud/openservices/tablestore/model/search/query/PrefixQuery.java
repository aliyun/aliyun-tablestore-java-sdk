package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 匹配前缀。比如搜索“name”是以“王”字开头的所有人。
 */
public class PrefixQuery implements Query {

    private String fieldName;
    /**
     * 	字符串前缀
     */
    private String prefix;

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_PrefixQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildPrefixQuery(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
