package com.alicloud.openservices.tablestore.model.search.query;

import com.google.protobuf.ByteString;

/**
 * Query接口，具体介绍请查看具体的实现类的说明
 */
public interface Query {

    QueryType getQueryType();

    ByteString serialize();

}
