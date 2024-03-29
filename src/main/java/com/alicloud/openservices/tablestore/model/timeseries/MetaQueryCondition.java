package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.google.protobuf.ByteString;

public interface MetaQueryCondition {

    Timeseries.MetaQueryConditionType getType();

    ByteString serialize();
}
