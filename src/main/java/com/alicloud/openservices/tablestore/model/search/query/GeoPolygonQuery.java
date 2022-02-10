package com.alicloud.openservices.tablestore.model.search.query;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 找出落在指定多边形包围起来的图形内的数据
 * <p>注意：这个查询器使用代价很大，请避免使用</p>
 * <p>场景举例：小黄车只能在繁华的地方服务，出了市区要收额外的服务费，而繁华的城市的边界是多边形的。我们想查询该车辆是否需要付额外的服务费，就需要通过搜索用户的经纬度是否在多边形内。</p>
 */
public class GeoPolygonQuery implements Query {

    private QueryType queryType = QueryType.QueryType_GeoPolygonQuery;

    /**
     * 字段名
     */
    private String fieldName;
    /**
     * 经纬度字符串的List
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
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildGeoPolygonQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private List<String> points;

        private Builder() {}

        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder addPoint(String pointString) {
            if (this.points == null) {
                this.points = new ArrayList<String>();
            }
            this.points.add(pointString);
            return this;
        }

        @Override
        public GeoPolygonQuery build() {
            GeoPolygonQuery geoPolygonQuery = new GeoPolygonQuery();
            geoPolygonQuery.setFieldName(this.fieldName);
            geoPolygonQuery.setPoints(this.points);
            return geoPolygonQuery;
        }

    }
}
