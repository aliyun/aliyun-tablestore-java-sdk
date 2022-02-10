package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 类似 {@link MatchQuery} （MatchQuery 仅匹配某个词即可），但是 MatchPhraseQuery会匹配所有的短语。
 */
public class MatchPhraseQuery implements Query {

    private QueryType queryType = QueryType.QueryType_MatchPhraseQuery;

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
