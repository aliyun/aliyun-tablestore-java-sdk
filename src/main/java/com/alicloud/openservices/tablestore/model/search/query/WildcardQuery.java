package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 通配符查询。支持 *（ 任意0或多个）和 ？（任意1个字符）。
 * <p>举例：名字字段是“name”，想查询名字中包含“龙”的人，就可以“*龙*” ，但是效率可能不高。</p>
 */
public class WildcardQuery implements Query {

    private QueryType queryType = QueryType.QueryType_WildcardQuery;

    private String fieldName;
    private String value;
    private float weight = 1.0f;

    @Override
    public QueryType getQueryType() {
        return queryType;
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

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private String value;
        private float weight = 1.0f;

        public Builder weight(float weight) {
            this.weight = weight;
            return this;
        }

        private Builder() {}

        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder value(String value) {
            this.value = value;
            return this;
        }

        @Override
        public WildcardQuery build() {
            WildcardQuery wildcardQuery = new WildcardQuery();
            wildcardQuery.setFieldName(this.fieldName);
            wildcardQuery.setValue(this.value);
            wildcardQuery.setWeight(this.weight);
            return wildcardQuery;
        }
    }
}
