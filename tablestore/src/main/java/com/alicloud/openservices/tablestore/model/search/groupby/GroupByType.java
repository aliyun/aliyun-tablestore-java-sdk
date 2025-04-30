package com.alicloud.openservices.tablestore.model.search.groupby;

import com.alicloud.openservices.tablestore.model.search.FieldType;

public enum GroupByType {
    /**
     * GroupBy based on the field
     */
    GROUP_BY_FIELD,
    /**
     * GroupBy based on the range
     */
    GROUP_BY_RANGE,
    /**
     * Perform groupBy based on the filter
     */
    GROUP_BY_FILTER,
    /**
     * Perform groupBy based on latitude and longitude.
     */
    GROUP_BY_GEO_DISTANCE,
    /**
     * Perform histogram statistics on the data
     */
    GROUP_BY_HISTOGRAM,
    /**
     * Perform histogram statistics on date type {@link FieldType#DATE} data
     */
    GROUP_BY_DATE_HISTOGRAM,
    /**
     * GroupBy geographical coordinates based on GeoHash
     */
    GROUP_BY_GEO_GRID,

    /**
     * Combine multiple GroupBy types. Currently, it supports the combination of {@link GroupByField}, {@link GroupByHistogram}, {@link GroupByDateHistogram}.
     */
    GROUP_BY_COMPOSITE,
}
