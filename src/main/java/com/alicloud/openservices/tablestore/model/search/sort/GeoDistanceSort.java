package com.alicloud.openservices.tablestore.model.search.sort;

import java.util.List;

/**
 * 地理空间模型中对距离的排序
 */
public class GeoDistanceSort implements Sort.Sorter {

    /**
     * 排序的字段
     */
    private String fieldName;
    /**
     * 排序的地理位置点
     */
    private List<String> points;
    /**
     * 升序或降序
     */
    private SortOrder order;
    /**
     * 多值字段的排序依据
     */
    private SortMode mode;
    /**
     * 计算两点距离的算法
     */
    private GeoDistanceType distanceType;
    /**
     * 嵌套的过滤器
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
