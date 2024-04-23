package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 匹配前缀。比如搜索“name”是以“王”字开头的所有人。
 */
public class PrefixQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_PrefixQuery;

    private String fieldName;
    /**
     * 	字符串前缀
     */
    private String prefix;

    private float weight = 1.0f;

    @Override
    public QueryType getQueryType() {
        return queryType;
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
        private String prefix;
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

        public Builder prefix(String prefix) {
            this.prefix = prefix;
            return this;
        }

        @Override
        public PrefixQuery build() {
            PrefixQuery prefixQuery = new PrefixQuery();
            prefixQuery.setFieldName(this.fieldName);
            prefixQuery.setPrefix(this.prefix);
            prefixQuery.setWeight(this.weight);
            return prefixQuery;
        }
    }
}
