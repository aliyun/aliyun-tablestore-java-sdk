package com.alicloud.openservices.tablestore.model.search.groupby;

import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

public interface GroupBy {

    String getGroupByName();

    GroupByType getGroupByType();

    ByteString serialize();
}
