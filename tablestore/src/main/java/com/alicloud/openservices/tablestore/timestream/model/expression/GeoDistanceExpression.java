package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.GeoDistanceQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;

/**
 * Created by yanglian on 2019/4/9.
 */
public class GeoDistanceExpression implements Expression {
    private GeoDistanceQuery query;

    public GeoDistanceExpression(String pos, double distance) {
        query = new GeoDistanceQuery();
        query.setCenterPoint(pos);
        query.setDistanceInMeter(distance);
    }

    @Override
    public Query getQuery(String columnName) {
        query.setFieldName(columnName);
        return query;
    }
}
