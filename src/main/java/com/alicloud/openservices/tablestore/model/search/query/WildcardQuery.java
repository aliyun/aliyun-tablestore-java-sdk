package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 通配符查询。支持 *（ 任意0或多个）和 ？（任意1个字符）。
 * <p>举例：名字字段是“name”，想查询名字中包含“龙”的人，就可以“*龙*” ，但是效率可能不高。</p>
 */
public class WildcardQuery implements Query {

    private String fieldName;
    private String value;

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_WildcardQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildWildcardQuery(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
