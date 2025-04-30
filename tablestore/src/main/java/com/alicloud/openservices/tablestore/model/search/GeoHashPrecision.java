package com.alicloud.openservices.tablestore.model.search;

/**
 * Represents the precision of a bucket in GroupByGeoGrid. The two numbers in each precision level name indicate the size of the rectangle corresponding to the bucket. 
 * Since the range represented by geoHash shrinks as it approaches the poles, the length in the enumeration name represents the size of the rectangle at the equator for that precision level. 
 * Additionally, the last number indicates the GeoHash length corresponding to this precision level.
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
