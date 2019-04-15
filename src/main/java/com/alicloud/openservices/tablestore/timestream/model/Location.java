package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;

public class Location {
    /**
     * 纬度
     */
    private double latitude;
    /**
     * 经度
     */
    private double longitude;
    private static String DECOLLATOR = ",";

    public Location() {
    }

    public Location(String str) {
        String[] subStr = str.split(",");
        if (subStr.length != 2) {
            throw new ClientException("Illegal string for location.");
        }
        this.latitude = Double.parseDouble(subStr[0]);
        this.longitude =  Double.parseDouble(subStr[1]);
    }

    public Location(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public Location setLatitude(double latitude) {
        this.latitude = latitude;
        return this;
    }

    public double getLatitude() {
        return this.latitude;
    }

    public Location setLongitude(double longitude) {
        this.longitude = longitude;
        return this;
    }

    public double getLongitude() {
        return this.longitude;
    }

    @Override
    public int hashCode() {
        long lat = Double.doubleToLongBits(this.latitude);
        long lon = Double.doubleToLongBits(this.longitude);
        return (int)(lat * 31 + lon);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof Location)) {
            return false;
        }
        Location val = (Location)o;
        return this.latitude == val.latitude && this.longitude == val.longitude;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.latitude);
        sb.append(DECOLLATOR);
        sb.append(this.longitude);
        return sb.toString();
    }
}
