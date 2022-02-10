package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * 聚合结果的接口，具体说明请看具体实现里的说明
 */
public interface AggregationResult {

    String getAggName();

    AggregationType getAggType();

}
