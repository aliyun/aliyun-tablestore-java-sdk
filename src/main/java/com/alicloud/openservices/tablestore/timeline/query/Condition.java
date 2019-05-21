package com.alicloud.openservices.tablestore.timeline.query;

import com.alicloud.openservices.tablestore.model.search.query.Query;

public interface Condition {
    Query getQuery();
}
