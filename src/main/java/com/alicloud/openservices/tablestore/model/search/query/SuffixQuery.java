package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * suffix query, for field type fuzzy_keyword only
 */
public class SuffixQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_SuffixQuery;

    public String fieldName;

    private String suffix;

    private float weight = 1.0f;

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildSuffixQuery(this).toByteString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    protected static SuffixQuery.Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private String suffix;
        private float weight = 1.0f;

        public SuffixQuery.Builder weight(float weight) {
            this.weight = weight;
            return this;
        }

        private Builder() {}

        public SuffixQuery.Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public SuffixQuery.Builder suffix(String suffix) {
            this.suffix = suffix;
            return this;
        }

        @Override
        public SuffixQuery build() {
            SuffixQuery suffixQuery = new SuffixQuery();
            suffixQuery.setFieldName(this.fieldName);
            suffixQuery.setSuffix(this.suffix);
            suffixQuery.setWeight(this.weight);
            return suffixQuery;
        }
    }
}
