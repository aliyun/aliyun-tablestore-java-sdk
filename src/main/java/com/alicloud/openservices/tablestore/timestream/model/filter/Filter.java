package com.alicloud.openservices.tablestore.timestream.model.filter;

import com.alicloud.openservices.tablestore.model.search.query.Query;

public interface Filter {
    public Query getQuery();
}
