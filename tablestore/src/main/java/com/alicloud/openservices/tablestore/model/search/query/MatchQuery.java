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
     * Minimum number of matches (deprecated).
     * <p>
     * This field is deprecated because the minimumShouldMatch parameter
     * can only accept integer and do not support percentages.
     * Use {@link #minShouldMatch} instead which stores the value as a String.
     *
     * @deprecated use {@link #minShouldMatch} instead
     */
    @Deprecated
    private Integer minimumShouldMatch;

    /**
     * Minimum number of matches as a String.
     * <p>
     * This field stores the minimumShouldMatch parameter as a String which can be
     * either an integer (e.g., "3") or a percentage (e.g., "50%").
     */
    private String minShouldMatch;

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

    /**
     * Get the minimum number of matches required as an Integer.
     * <p>
     * This method is deprecated because the minimumShouldMatch parameter
     * can only accept integer values but percentage strings (e.g., "50%").
     *
     * @return the minimum number of matches as an Integer, or null if not set
     * @throws IllegalStateException if the stored value is not a valid integer
     * @deprecated use {@link #getMinShouldMatch()} instead which returns the value as a String
     */
    @Deprecated
    public Integer getMinimumShouldMatch() {
        return minimumShouldMatch;
    }

    /**
     * Get the minimum number of matches required as a String.
     * <p> 
     * This method returns the raw string value of minimumShouldMatch which can be
     * either an integer (e.g., "3") or a percentage (e.g., "50%"). This provides
     * more flexibility than {@link #getMinimumShouldMatch()} which only supports integers.
     * <p>
     * In a later version, this method will be renamed to getMinimumShouldMatch().
     *
     * @return the minimum number of matches as a String, or null if not set
     */
    public String getMinShouldMatch() {
        return minShouldMatch;
    }

    /**
     * Set the minimum number of matches required as an Integer.
     *
     * @param minimumShouldMatch the minimum number of matches as an Integer
     * @deprecated use {@link #setMinShouldMatch(String)} or {@link #setMinShouldMatch(int)} instead
     */
    @Deprecated
    public void setMinimumShouldMatch(Integer minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    /**
     * Set the minimum number of matches required as a String.
     * <p>
     * This method accepts the minimumShouldMatch parameter as a String which can be
     * either an integer (e.g., "3") or a percentage (e.g., "50%").
     *
     * @param minShouldMatch the minimum number of matches as a String
     */
    public void setMinShouldMatch(String minShouldMatch) {
        this.minShouldMatch = minShouldMatch;
    }

    public void setMinShouldMatch(int minShouldMatch) {
        this.minShouldMatch = String.valueOf(minShouldMatch);
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
        @Deprecated
        private Integer minimumShouldMatch;
        private QueryOperator operator;
        private float weight = 1.0f;
        private String minShouldMatch;

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

        @Deprecated
        public Builder minimumShouldMatch(int minimumShouldMatch) {
            this.minimumShouldMatch = minimumShouldMatch;
            return this;
        }

        public Builder minShouldMatch(String minShouldMatch) {
            this.minShouldMatch = minShouldMatch;
            return this;
        }

        public Builder minShouldMatch(int minShouldMatch) {
            this.minShouldMatch = String.valueOf(minShouldMatch);
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
            matchQuery.setMinShouldMatch(this.minShouldMatch);
            return matchQuery;
        }
    }
}
