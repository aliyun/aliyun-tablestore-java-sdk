package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.google.protobuf.ByteString;

/**
 * 找出与某个位置某个距离内的数据。
 * <p>常用场景：搜索我附近1千米内的人。</p>
 * <p>通过设置我的centerPoint（一个经纬度信息），然后设置举例信息distanceInMeter=1000，进行查询即可。</p>
 */
public class GeoDistanceQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_GeoDistanceQuery;

    /**
     * 字段名
     */
    private String fieldName;
    /**
     * 中心点
     */
    private String centerPoint;
    /**
     * 与中心点的距离（单位：米）
     */
    private double distanceInMeter;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getCenterPoint() {
        return centerPoint;
    }

    public void setCenterPoint(String centerPoint) {
        this.centerPoint = centerPoint;
    }

    public double getDistanceInMeter() {
        return distanceInMeter;
    }

    public void setDistanceInMeter(double distanceInMeter) {
        this.distanceInMeter = distanceInMeter;
    }

    @Override
    public QueryType getQueryType() {
        return queryType;
    }

    @Override
    public ByteString serialize() {
        return SearchQueryBuilder.buildGeoDistanceQuery(this).toByteString();
    }

    protected static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder implements QueryBuilder {
        private String fieldName;
        private String centerPoint;
        private double distanceInMeter;

        private Builder() {}

        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder centerPoint(String centerPoint) {
            this.centerPoint = centerPoint;
            return this;
        }

        public Builder distanceInMeter(double distanceInMeter) {
            this.distanceInMeter = distanceInMeter;
            return this;
        }

        @Override
        public GeoDistanceQuery build() {
            GeoDistanceQuery geoDistanceQuery = new GeoDistanceQuery();
            geoDistanceQuery.setCenterPoint(this.centerPoint);
            geoDistanceQuery.setDistanceInMeter(this.distanceInMeter);
            geoDistanceQuery.setFieldName(this.fieldName);
            return geoDistanceQuery;
        }
    }
}
