package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

public class AttributeMetaQueryCondition implements MetaQueryCondition {

    private final MetaQuerySingleOperator operator;
    private final String attributeName;
    private final String value;

    public AttributeMetaQueryCondition(MetaQuerySingleOperator operator, String attributeName, String value) {
        Preconditions.checkNotNull(operator);
        Preconditions.checkStringNotNullAndEmpty(attributeName, "tag name should not be null or empty");
        Preconditions.checkNotNull(value);
        this.operator = operator;
        this.attributeName = attributeName;
        this.value = value;
    }

    @Override
    public Timeseries.MetaQueryConditionType getType() {
        return Timeseries.MetaQueryConditionType.ATTRIBUTE_CONDITION;
    }

    @Override
    public ByteString serialize() {
        return Timeseries.MetaQueryAttributeCondition.newBuilder()
                .setOp(operator.toPB())
                .setAttrName(attributeName)
                .setValue(value)
                .build()
                .toByteString();
    }
}
