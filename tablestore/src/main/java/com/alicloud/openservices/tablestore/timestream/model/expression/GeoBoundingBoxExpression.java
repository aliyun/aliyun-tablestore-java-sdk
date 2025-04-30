package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoPolygonQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;

import java.util.Arrays;
import java.util.List;

/**
 * Created by yanglian on 2019/4/9.
 */
public class GeoBoundingBoxExpression implements Expression  {
    private GeoBoundingBoxQuery query;

    public GeoBoundingBoxExpression(String topLeftPos, String bottomRightPos) {
        query = new GeoBoundingBoxQuery();
        query.setTopLeft(topLeftPos);
        query.setBottomRight(bottomRightPos);
    }

    @Override
    public Query getQuery(String columnName) {
        query.setFieldName(columnName);
        return query;
    }
}
