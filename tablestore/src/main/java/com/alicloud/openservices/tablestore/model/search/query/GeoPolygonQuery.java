package com.alicloud.openservices.tablestore.model.search.query;

import java.util.ArrayList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.protocol.SearchQueryBuilder;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Find the data within the shape enclosed by the specified polygon.
 * <p>Note: This query has a high cost and should be avoided if possible.</p>
 * <p>Use case example: A bike-sharing service only operates in bustling urban areas, and an extra service fee is charged outside the city limits. The boundary of the urban area is defined by a polygon. To determine whether the bike is inside or outside this area (and thus whether an extra fee is required), we need to check if the user's latitude and longitude fall within the polygon.</p>
 */
public class GeoPolygonQuery implements Query {

    private final QueryType queryType = QueryType.QueryType_GeoPolygonQuery;

    /**
     * Field name
     */
    private String fieldName;
    /**
     * List of latitude and longitude strings
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
