package com.alicloud.openservices.tablestore.model.search.agg;

import com.google.protobuf.ByteString;

/**
 * agg的接口，具体说明请看具体实现里的说明
 */
public interface Aggregation {

    String getAggName();

    AggregationType getAggType();

    ByteString serialize();
}
