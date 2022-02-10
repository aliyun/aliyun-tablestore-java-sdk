package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 包括模糊匹配和短语或邻近查询
 */
public class MatchQuery implements Query {

    private QueryType queryType = QueryType.QueryType_MatchQuery;

    /**
     * 字段
     */
    private String fieldName;
    /**
     * 模糊匹配的值
     */
    private String text;

    private float weight = 1.0f;

    /**
     * 最小匹配个数
     *
     * @return
     */
    private Integer minimumShouldMatch;

    /**
     * 操作符
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
         * 设置操作符(非必要操作)
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
