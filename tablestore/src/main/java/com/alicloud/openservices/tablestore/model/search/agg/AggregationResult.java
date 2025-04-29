package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * The interface of aggregation results. For detailed descriptions, please refer to the explanations in specific implementations.
 */
public interface AggregationResult {

    String getAggName();

    AggregationType getAggType();

}
