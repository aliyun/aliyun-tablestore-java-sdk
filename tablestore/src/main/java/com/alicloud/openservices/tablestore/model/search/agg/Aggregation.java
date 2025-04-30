package com.alicloud.openservices.tablestore.model.search.agg;

import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * The interface for aggregation. For detailed explanations, please refer to the specific implementation.
 */
public interface Aggregation {

    String getAggName();

    AggregationType getAggType();

    ByteString serialize();
}
