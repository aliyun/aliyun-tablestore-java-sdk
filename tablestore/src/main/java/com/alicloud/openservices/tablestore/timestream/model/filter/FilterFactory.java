package com.alicloud.openservices.tablestore.timestream.model.filter;

public class FilterFactory {

    public static Filter and(Filter... filters) {
        return new AndFilter(filters);
    }

    public static Filter or(Filter... filters) {
        return new OrFilter(filters);
    }
}
