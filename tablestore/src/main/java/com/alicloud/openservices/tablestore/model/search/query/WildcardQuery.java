package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Wildcard query. Supports * (any 0 or more characters) and ? (any single character).
 * <p>Example: If the name field is "name" and you want to query people whose names contain "Long", you can use "*Long*". However, this may not be efficient.</p>
 */
public class WildcardQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_WildcardQuery;

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
