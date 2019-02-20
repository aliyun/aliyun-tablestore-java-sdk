package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 类似 {@link MatchQuery} （MatchQuery 仅匹配某个词即可），但是 MatchPhraseQuery会匹配所有的短语。
 */
public class MatchPhraseQuery implements Query {

    private String fieldName;
    private String text;

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

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_MatchPhraseQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildMatchPhraseQuery(this).toByteString();
    }
}
