package com.alicloud.openservices.tablestore.model.search.sort;

/**
 * The model used by the Field of geographical spatial type when calculating distance (default is ARC)
 */
public enum GeoDistanceType {
    /**
     * Arc (since the earth is curved, this setting can accurately measure distances, but involves a larger amount of computation)
     */
    ARC,
    /**
     * Consider the earth as a plane and calculate the distance between two points (not precise enough, but with less computation)
     */
    PLANE
}
