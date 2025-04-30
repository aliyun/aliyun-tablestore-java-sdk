package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Get all documents, and the score of all documents is 1. In the returned results: the number of hits is always accurate. If there are too many results to return, SearchIndex will only return partial data.
 */
public class MatchAllQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_MatchAllQuery;

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildMatchAllQuery().toByteString();
    }

    protected static MatchAllQuery.Builder newBuilder() {
        return new MatchAllQuery.Builder();
    }

    public static final class Builder implements QueryBuilder {

        @Override
        public Query build() {
            return new MatchAllQuery();
        }
    }
}
