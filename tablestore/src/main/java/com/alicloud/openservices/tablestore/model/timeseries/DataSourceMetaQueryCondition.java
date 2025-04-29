package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

public class DataSourceMetaQueryCondition implements MetaQueryCondition {

    private final MetaQuerySingleOperator operator;
    private final String value;

    public DataSourceMetaQueryCondition(MetaQuerySingleOperator operator, String value) {
        Preconditions.checkNotNull(operator);
        Preconditions.checkNotNull(value);
        this.operator = operator;
        this.value = value;
    }

    @Override
    public Timeseries.MetaQueryConditionType getType() {
        return Timeseries.MetaQueryConditionType.SOURCE_CONDITION;
    }

    @Override
    public ByteString serialize() {
        return Timeseries.MetaQuerySourceCondition.newBuilder()
            .setOp(operator.toPB())
            .setValue(value)
            .build()
            .toByteString();
    }
}
