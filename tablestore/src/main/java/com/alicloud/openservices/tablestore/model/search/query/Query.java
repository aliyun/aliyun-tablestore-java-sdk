package com.alicloud.openservices.tablestore.model.search.query;

import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Query interface, for more details please refer to the description of the specific implementation class.
 */
public interface Query {

    QueryType getQueryType();

    ByteString serialize();

}
