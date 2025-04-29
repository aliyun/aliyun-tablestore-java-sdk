package com.alicloud.openservices.tablestore.model.search.filter;

import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilder;

public class SearchFilter {
    /**
     * Filter conditions in the filter
     */
    Query query;

    public static Builder newBuilder() {
        return new Builder();
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public SearchFilter() {}

    private SearchFilter(Builder builder) {
        setQuery(builder.query);
    }

    public static final class Builder {
        private Query query;

        private Builder() {}

        public Builder query(Query query) {
            this.query = query;
            return this;
        }

        public Builder query(QueryBuilder queryBuilder) {
            this.query = queryBuilder.build();
            return this;
        }

        public SearchFilter build() {
            return new SearchFilter(this);
        }
    }
}
