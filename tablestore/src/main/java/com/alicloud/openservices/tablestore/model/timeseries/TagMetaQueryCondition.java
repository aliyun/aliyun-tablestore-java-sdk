package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

public class TagMetaQueryCondition implements MetaQueryCondition {

    private final MetaQuerySingleOperator operator;
    private final String tagName;
    private final String value;

    public TagMetaQueryCondition(MetaQuerySingleOperator operator, String tagName, String value) {
        Preconditions.checkNotNull(operator);
        Preconditions.checkStringNotNullAndEmpty(tagName, "tag name should not be null or empty");
        Preconditions.checkNotNull(value);
        this.operator = operator;
        this.tagName = tagName;
        this.value = value;
    }

    @Override
    public Timeseries.MetaQueryConditionType getType() {
        return Timeseries.MetaQueryConditionType.TAG_CONDITION;
    }

    @Override
    public ByteString serialize() {
        return Timeseries.MetaQueryTagCondition.newBuilder()
            .setOp(operator.toPB())
            .setTagName(tagName)
            .setValue(value)
            .build()
            .toByteString();
    }
}
