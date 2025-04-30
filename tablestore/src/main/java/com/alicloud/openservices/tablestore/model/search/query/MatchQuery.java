package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Includes fuzzy matching and phrase or proximity queries.
 */
public class MatchQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_MatchQuery;

    /**
     * Field
     */
    private String fieldName;
    /**
     * Fuzzy matching value
     */
    private String text;

    private float weight = 1.0f;

    /**
     * Minimum number of matches
     *
     * @return
     */
    private Integer minimumShouldMatch;

    /**
     * Operator
     *
     * @return
     */
    private QueryOperator operator;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public float getWeight() {
        return weight;
    }

    public void setWeight(float weight) {
        this.weight = weight;
    }

    public QueryOperator getOperator() {
        return operator;
    }

    public void setOperator(QueryOperator operator) {
        this.operator = operator;
    }

    public Integer getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    public void setMinimumShouldMatch(Integer minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildMatchQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private String text;
        private Integer minimumShouldMatch;
        private QueryOperator operator;
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

        public Builder text(String text) {
            this.text = text;
            return this;
        }

        public Builder minimumShouldMatch(int minimumShouldMatch) {
            this.minimumShouldMatch = minimumShouldMatch;
            return this;
        }

        /**
         * Set the operator (non-essential operation)
         */
        public Builder operator(QueryOperator queryOperator) {
            this.operator = queryOperator;
            return this;
        }

        @Override
        public MatchQuery build() {
            MatchQuery matchQuery = new MatchQuery();
            matchQuery.setFieldName(this.fieldName);
            matchQuery.setMinimumShouldMatch(this.minimumShouldMatch);
            matchQuery.setText(this.text);
            matchQuery.setWeight(this.weight);
            matchQuery.setOperator(this.operator);
            return matchQuery;
        }
    }
}
