package com.alicloud.openservices.tablestore.model.search.agg;

/**
 * 所有 agg 的 innerBuilder 继承自 AggregationBuilder
 */
public interface AggregationBuilder {

    Aggregation build();
}
