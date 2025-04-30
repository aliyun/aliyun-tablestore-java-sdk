package com.alicloud.openservices.tablestore.timestream.model.filter;

import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * OR combination filter
 */
public class OrFilter implements Filter {
    private List<Filter> filterList;

    public OrFilter(Filter... filters) {
        this.filterList = Arrays.asList(filters);
    }

    public Query getQuery() {
        List<Query> queryList = new ArrayList<Query>();
        for (Filter c : filterList) {
            queryList.add(c.getQuery());
        }
        BoolQuery boolQuery = new BoolQuery();
        boolQuery.setShouldQueries(queryList);
        return boolQuery;
    }
}
