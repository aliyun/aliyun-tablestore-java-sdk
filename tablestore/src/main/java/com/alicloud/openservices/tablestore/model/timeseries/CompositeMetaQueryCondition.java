package com.alicloud.openservices.tablestore.model.timeseries;

import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class CompositeMetaQueryCondition implements MetaQueryCondition {

    private final MetaQueryCompositeOperator operator;
    private List<MetaQueryCondition> subConditions = new ArrayList<MetaQueryCondition>();

    public CompositeMetaQueryCondition(MetaQueryCompositeOperator op) {
        Preconditions.checkNotNull(op);
        this.operator = op;
    }

    public List<MetaQueryCondition> getSubConditions() {
        return subConditions;
    }

    public void setSubConditions(List<MetaQueryCondition> subConditions) {
        this.subConditions = subConditions;
    }

    public void addSubCondition(MetaQueryCondition subCondition) {
        this.subConditions.add(subCondition);
    }

    @Override
    public Timeseries.MetaQueryConditionType getType() {
        return Timeseries.MetaQueryConditionType.COMPOSITE_CONDITION;
    }

    @Override
    public ByteString serialize() {
        Preconditions.checkArgument(!subConditions.isEmpty(), "subConditions is empty");
        Timeseries.MetaQueryCompositeCondition.Builder builder =
            Timeseries.MetaQueryCompositeCondition.newBuilder().setOp(operator.toPB());
        for (MetaQueryCondition condition : subConditions) {
            builder.addSubConditions(Timeseries.MetaQueryCondition.newBuilder()
                    .setType(condition.getType())
                    .setProtoData(condition.serialize()));
        }
        return builder.build().toByteString();
    }
}
