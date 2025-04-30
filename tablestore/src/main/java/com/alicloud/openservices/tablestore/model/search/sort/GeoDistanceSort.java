package com.alicloud.openservices.tablestore.model.search.sort;

import java.util.List;

/**
 * Sorting of distances in the geospatial model
 */
public class GeoDistanceSort implements Sort.Sorter {

    /**
     * The field for sorting
     */
    private String fieldName;
    /**
     * Sorted geographical location points
     */
    private List<String> points;
    /**
     * Ascending or descending order
     */
    private SortOrder order;
    /**
     * Sorting criteria for multi-value fields
     */
    private SortMode mode;
    /**
     * Algorithm for calculating the distance between two points
     */
    private GeoDistanceType distanceType;
    /**
     * Nested filters
     */
    private NestedFilter nestedFilter;

    public GeoDistanceSort(String fieldName, List<String> points) {
        this.fieldName = fieldName;
        this.points = points;
    }

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

    public SortOrder getOrder() {
        return order;
    }

    public void setOrder(SortOrder order) {
        this.order = order;
    }

    public SortMode getMode() {
        return mode;
    }

    public void setMode(SortMode mode) {
        this.mode = mode;
    }

    public GeoDistanceType getDistanceType() {
        return distanceType;
    }

    public void setDistanceType(GeoDistanceType distanceType) {
        this.distanceType = distanceType;
    }

    public NestedFilter getNestedFilter() {
        return nestedFilter;
    }

    public void setNestedFilter(NestedFilter nestedFilter) {
        this.nestedFilter = nestedFilter;
    }
}
