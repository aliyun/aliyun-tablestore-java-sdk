package com.alicloud.openservices.tablestore.model.search.query;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Find data within a certain distance from a given location.
 * <p>Common use case: Search for people within 1 kilometer of my location.</p>
 * <p>By setting my centerPoint (a geographic coordinate), and then setting the distance information as distanceInMeter=1000, the query can be performed.</p>
 */
public class GeoDistanceQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_GeoDistanceQuery;

    /**
     * Field name
     */
    private String fieldName;
    /**
     * Center point
     */
    private String centerPoint;
    /**
     * Distance from the center point (unit: meters)
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
