package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

public class UpdateTimeMetaQueryCondition implements MetaQueryCondition {

    private final MetaQuerySingleOperator operator;
    private final long timeInUs;

    public UpdateTimeMetaQueryCondition(MetaQuerySingleOperator operator, long timeInUs) {
        Preconditions.checkNotNull(operator);
        this.operator = operator;
        this.timeInUs = timeInUs;
    }

    @Override
    public Timeseries.MetaQueryConditionType getType() {
        return Timeseries.MetaQueryConditionType.UPDATE_TIME_CONDITION;
    }

    @Override
    public ByteString serialize() {
        return Timeseries.MetaQueryUpdateTimeCondition.newBuilder()
            .setOp(operator.toPB())
            .setValue(timeInUs)
            .build()
            .toByteString();
    }
}
