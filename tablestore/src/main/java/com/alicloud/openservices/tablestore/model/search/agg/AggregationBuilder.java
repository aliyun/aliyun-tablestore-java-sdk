package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * All agg's innerBuilder inherit from AggregationBuilder
 */
public interface AggregationBuilder {

    Aggregation build();
}
