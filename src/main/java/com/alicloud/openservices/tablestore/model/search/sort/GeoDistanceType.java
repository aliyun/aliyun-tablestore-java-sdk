package com.alicloud.openservices.tablestore.model.search.sort;

/**
 * 地理空间类型的Field在计算距离的时候采用的模型（默认为ARC）
 */
public enum GeoDistanceType {
    /**
     * 弧形（因为地球是弧形的，采用该设置可精确距离，但计算量较大）
     */
    ARC,
    /**
     * 把地球看成平面，计算两点距离（精确值不够，但是计算量小）
     */
    PLANE
}
