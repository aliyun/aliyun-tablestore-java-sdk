package com.alicloud.openservices.tablestore.model.search.groupby;

public enum GroupByType {
    /**
     * 根据 field 进行 groupBy
     */
    GROUP_BY_FIELD,
    /**
     * 根据范围进行 groupBy
     */
    GROUP_BY_RANGE,
    /**
     * 根据filter进行 groupBy
     */
    GROUP_BY_FILTER,
    /**
     * 根据经纬度进行 groupBy
     */
    GROUP_BY_GEO_DISTANCE,
    /**
     * 对数据进行直方图统计
     */
    GROUP_BY_HISTOGRAM,

}
