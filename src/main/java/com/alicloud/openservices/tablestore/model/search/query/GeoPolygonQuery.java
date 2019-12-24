package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * 找出落在指定多边形包围起来的图形内的数据
 * <p>场景举例：小黄车只能在繁华的地方服务，出了市区要收额外的服务费，而繁华的城市的边界是多边形的。我们想查询该车辆是否需要付额外的服务费，就需要通过搜索用户的经纬度是否在多边形内。</p>
 */
public class GeoPolygonQuery implements Query {

    /**
     * 字段名
     */
    private String fieldName;
    /**
     *  经纬度字符串的List
     */
    private List<String> points;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public List<String> getPoints() {
        return points;
    }

    public void setPoints(List<String> points) {
        this.points = points;
    }

    @Override
    public QueryType getQueryType() {
        return QueryType.QueryType_GeoPolygonQuery;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildGeoPolygonQuery(this).toByteString();
    }
}
