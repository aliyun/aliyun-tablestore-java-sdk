package com.alicloud.openservices.tablestore.timestream.model.condition;

import com.alicloud.openservices.tablestore.model.search.query.Query;

public interface Condition {
    public Query getQuery();
}
