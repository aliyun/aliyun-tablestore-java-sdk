package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Similar to {@link MatchQuery} (MatchQuery only matches a single word), but MatchPhraseQuery matches the entire phrase.
 */
public class MatchPhraseQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_MatchPhraseQuery;

    private String fieldName;
    private String text;
    private float weight = 1.0f;

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

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildMatchPhraseQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder{
        private String fieldName;
        private String text;
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

        @Override
        public MatchPhraseQuery build() {
            MatchPhraseQuery matchAllQuery = new MatchPhraseQuery();
            matchAllQuery.setFieldName(this.fieldName);
            matchAllQuery.setText(this.text);
            matchAllQuery.setWeight(this.weight);
            return matchAllQuery;
        }
    }
}
