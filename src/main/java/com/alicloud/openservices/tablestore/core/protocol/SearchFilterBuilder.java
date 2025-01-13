package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.filter.SearchFilter;

public class SearchFilterBuilder {
    public static Search.SearchFilter buildSearchFilter(SearchFilter filter) {
        Search.SearchFilter.Builder builder = Search.SearchFilter.newBuilder();
        if (filter.getQuery() != null) {
            builder.setQuery(SearchQueryBuilder.buildQuery(filter.getQuery()));
        }
        return builder.build();
    }
}
