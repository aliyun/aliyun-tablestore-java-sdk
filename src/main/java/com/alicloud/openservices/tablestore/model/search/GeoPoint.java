package com.alicloud.openservices.tablestore.model.search;

/**
 * 经纬度的实体类
 */
public class GeoPoint {

    private double lat;
    private double lon;

    public GeoPoint(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }
}
