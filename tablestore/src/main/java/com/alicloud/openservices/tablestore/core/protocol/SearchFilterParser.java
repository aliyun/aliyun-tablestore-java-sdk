package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.search.filter.SearchFilter;

import java.io.IOException;

public class SearchFilterParser {
    public static SearchFilter toSearchFilter(Search.SearchFilter pb) throws IOException {
        SearchFilter.Builder builder = SearchFilter.newBuilder();
        if (pb.hasQuery()) {
            builder.query(SearchQueryParser.toQuery(pb.getQuery()));
        }
        return builder.build();
    }
}
