package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Existence query for the 'exists' field
 */
public class ExistsQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_ExistsQuery;

    private String fieldName;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildExistsQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new ExistsQuery.Builder();
    }
    public static final class Builder implements QueryBuilder{
        private String fieldName;

        private Builder() {}

        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        @Override
        public ExistsQuery build() {
            ExistsQuery existsQuery= new ExistsQuery();
            existsQuery.setFieldName(fieldName);
            return existsQuery;
        }
    }
}
