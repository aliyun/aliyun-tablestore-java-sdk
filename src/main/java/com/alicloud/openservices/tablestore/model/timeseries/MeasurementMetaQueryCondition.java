package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.google.protobuf.ByteString;

public class MeasurementMetaQueryCondition implements MetaQueryCondition {

    private final MetaQuerySingleOperator operator;
    private final String value;

    public MeasurementMetaQueryCondition(MetaQuerySingleOperator operator, String value) {
        Preconditions.checkNotNull(operator);
        Preconditions.checkNotNull(value);
        this.operator = operator;
        this.value = value;
    }

    @Override
    public Timeseries.MetaQueryConditionType getType() {
        return Timeseries.MetaQueryConditionType.MEASUREMENT_CONDITION;
    }

    @Override
    public ByteString serialize() {
        return Timeseries.MetaQueryMeasurementCondition.newBuilder()
            .setOp(operator.toPB())
            .setValue(value)
            .build()
            .toByteString();
    }
}
