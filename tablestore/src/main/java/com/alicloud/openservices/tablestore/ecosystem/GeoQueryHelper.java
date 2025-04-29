package com.alicloud.openservices.tablestore.ecosystem;

import com.google.gson.Gson;

import java.util.List;

// Merge the contents of GeoDistanceQuery, GeoBoundingBoxQuery and GeoPolygonQuery
public class GeoQueryHelper {
    private static final Gson GSON = new Gson();
    /**
     * Field name
     */
    private String fieldName;

    // GeoDistanceQuery
    /**
     * Center point
     */
    private String centerPoint;
    /**
     * Distance from the center point (unit: meters)
     */
    private double distanceInMeter;

    // GeoBoundingBoxQuery
    /**
     * The latitude and longitude of the top-left corner of the rectangle.
     * <p>Example: "46.24123424, 23.2342424"</p>
     */
    private String topLeft;
    /**
     * The longitude and latitude of the bottom-right corner of the rectangle.
     * <p>Example: "46.24123424, 23.2342424"</p>
     */
    private String bottomRight;

    // GeoPolygonQuery
    /**
     * List of latitude and longitude strings
     */
    private List<String> points;

    private GeoType geoType;

    public static GeoQueryHelper buildGeoQueryHelper(Filter filter) {
        if (filter.getCompareOperator() != Filter.CompareOperator.EQUAL) {
            throw new IllegalArgumentException("geo column only support = json string,example: geoColumn = {centerPoint:6,9, distanceInMeter: 10000}");
        }
        String json = filter.getColumnValue().toString();
        GeoQueryHelper geoQuery = GSON.fromJson(json, GeoQueryHelper.class);
        if (geoQuery.getCenterPoint() != null && geoQuery.getDistanceInMeter() >= 0) {
            geoQuery.setGeoType(GeoType.DISTANCE);
        } else if (geoQuery.getTopLeft() != null && geoQuery.getBottomRight() != null) {
            geoQuery.setGeoType(GeoType.BOUNDING_BOX);
        } else if (geoQuery.getPoints() != null && !geoQuery.getPoints().isEmpty()) {
            geoQuery.setGeoType(GeoType.POLYGON);
        } else {
            throw new IllegalArgumentException("geo query must be distance, boundingbox or polygon format");
        }
        return geoQuery;
    }

    public GeoType getGeoType() {
        return geoType;
    }

    public void setGeoType(GeoType geoType) {
        this.geoType = geoType;
    }

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

    public String getTopLeft() {
        return topLeft;
    }

    public void setTopLeft(String topLeft) {
        this.topLeft = topLeft;
    }

    public String getBottomRight() {
        return bottomRight;
    }

    public void setBottomRight(String bottomRight) {
        this.bottomRight = bottomRight;
    }

    public List<String> getPoints() {
        return points;
    }

    public void setPoints(List<String> points) {
        this.points = points;
    }


}
