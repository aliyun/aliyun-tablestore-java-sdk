package com.alicloud.openservices.tablestore.timestream.model.expression;

import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoPolygonQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;

import java.util.Arrays;
import java.util.List;

/**
 * Created by yanglian on 2019/4/9.
 */
public class GeoPolygonExpression implements Expression {
    private GeoPolygonQuery query;

    public GeoPolygonExpression(List<String> polygonList) {
        query = new GeoPolygonQuery();
        query.setPoints(polygonList);
    }

    public GeoPolygonExpression(String[] polygonList) {
        query = new GeoPolygonQuery();
        query.setPoints(Arrays.asList(polygonList));
    }

    @Override
    public Query getQuery(String columnName) {
        query.setFieldName(columnName);
        return query;
    }
}
