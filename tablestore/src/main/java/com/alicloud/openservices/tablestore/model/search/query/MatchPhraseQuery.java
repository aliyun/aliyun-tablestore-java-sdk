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

    /**
     * The maximum number of positions allowed between matching tokens for phrases.
     * <p>
     * A slop of 0 requires an exact phrase match. A slop of 1 allows one word to be
     * swapped or one word to be inserted between the query terms. Higher values allow
     * more flexibility in matching.
     */
    private Integer slop;

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

    public Integer getSlop() {
        return slop;
    }

    public void setSlop(Integer slop) {
        this.slop = slop;
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

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private String text;
        private float weight = 1.0f;
        private Integer slop;

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

        public Builder slop(Integer slop) {
            this.slop = slop;
            return this;
        }

        @Override
        public MatchPhraseQuery build() {
            MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery();
            matchPhraseQuery.setFieldName(this.fieldName);
            matchPhraseQuery.setText(this.text);
            matchPhraseQuery.setWeight(this.weight);
            matchPhraseQuery.setSlop(this.slop);
            return matchPhraseQuery;
        }
    }
}
