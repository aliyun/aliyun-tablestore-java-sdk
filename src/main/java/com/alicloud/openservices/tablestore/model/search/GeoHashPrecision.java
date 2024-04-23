package com.alicloud.openservices.tablestore.model.search;

/**
 * 表示GroupByGeoGrid中bucket的精度，每个精度等级名称中的两个长度表示对应bucket的矩形大小，由于随着维度向两级靠拢，geoHash所代表的范围也会缩小，
 * 因此枚举类名称中的长度代表的是该精度在赤道附近表示的矩形大小。此外最后一个数字表示的是该精度对应的GeoHash长度
 */
public enum GeoHashPrecision {
    /**
     * unknown
     */
    UNKNOWN,
    /**
     * 5009km * 4922km
     */
    GHP_5009KM_4992KM_1,
    /**
     * 1252km * 624km
     */
    GHP_1252KM_624KM_2,
    /**
     * 156km * 156km
     */
    GHP_156KM_156KM_3,
    /**
     * 39km * 19km
     */
    GHP_39KM_19KM_4,
    /**
     * 4900m * 4900m
     */
    GHP_4900M_4900M_5,
    /**
     * 1200m * 609m
     */
    GHP_1200M_609M_6,
    /**
     * 152m * 152m
     */
    GHP_152M_152M_7,
    /**
     * 38m * 19m
     */
    GHP_38M_19M_8,
    /**
     * 480cm * 480cm
     */
    GHP_480CM_480CM_9,
    /**
     * 120cm * 595mm
     */
    GHP_120CM_595MM_10,
    /**
     * 149mm * 149mm
     */
    GHP_149MM_149MM_11,
    /**
     * 37mm * 19mm
     */
    GHP_37MM_19MM_12
}
