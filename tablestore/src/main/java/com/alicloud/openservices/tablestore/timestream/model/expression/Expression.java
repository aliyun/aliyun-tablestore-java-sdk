package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.Query;

public interface Expression {
    public Query getQuery(String columnName);
}
