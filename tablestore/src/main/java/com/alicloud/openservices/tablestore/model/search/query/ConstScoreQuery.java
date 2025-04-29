package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * When we don't care about the impact of the retrieval term frequency TF (Term Frequency) on the ranking of search results, we can use constant_score to wrap the query statement query or filter statement, which improves the search speed.
 * <p>Example: There are 100 people in our class, and there is a field called "name". We want to find people whose names contain "Wang", and we don't care about the sorting results. Using ConstScoreQuery (placing the original Query in "private Query filter;") will greatly improve the search speed.</p>
 */
public class ConstScoreQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_ConstScoreQuery;

    private Query filter;

    public Query getFilter() {
        return filter;
    }

    public void setFilter(Query filter) {
        this.filter = filter;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildConstScoreQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new ConstScoreQuery.Builder();
    }

    public static final class Builder implements QueryBuilder {
        private Query filter;

        private Builder() {}

        public Builder filter(QueryBuilder queryBuilder) {
            this.filter = queryBuilder.build();
            return this;
        }

        public Builder filter(Query query) {
            this.filter = query;
            return this;
        }

        @Override
        public ConstScoreQuery build() {
            ConstScoreQuery constScoreQuery = new ConstScoreQuery();
            constScoreQuery.setFilter(this.filter);
            return constScoreQuery;
        }
    }
}
