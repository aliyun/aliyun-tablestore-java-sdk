package com.alicloud.openservices.tablestore.model.search.sort;

import com.alicloud.openservices.tablestore.model.search.query.Query;

/**
 * A nested filter
 */
public class NestedFilter {

    private String path;
    private Query query;

    public NestedFilter(String path, Query query) {
        this.path = path;
        this.query = query;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }
}
