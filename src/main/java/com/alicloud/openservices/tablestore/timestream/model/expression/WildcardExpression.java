package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.query.WildcardQuery;

/**
 * Created by yanglian on 2019/4/9.
 */
public class WildcardExpression implements Expression  {
    private String value;

    public WildcardExpression(String value){
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public Query getQuery(String columnName) {
        WildcardQuery wildcardQuery = new WildcardQuery();
        wildcardQuery.setFieldName(columnName);
        wildcardQuery.setValue(value);
        return wildcardQuery;
    }
}
