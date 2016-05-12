package com.aliyun.openservices.ots.model.condition;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class CompositeCondition implements ColumnCondition {
    public static enum LogicOperator {
        NOT, AND, OR
    }

    private LogicOperator type;
    private List<ColumnCondition> subConditions;

    public CompositeCondition(LogicOperator loType) {
        this.type = loType;
        this.subConditions = new ArrayList<ColumnCondition>();
    }

    public CompositeCondition addCondition(ColumnCondition condition) {
        this.subConditions.add(condition);
        return this;
    }

    public void clear() {
        this.subConditions.clear();
    }

    /**
     * 查看当前设置的逻辑关系。
     *
     * @return 逻辑关系符号。
     */
    public LogicOperator getOperationType() {
        return this.type;
    }

    public List<ColumnCondition> getSubConditions() {
        return this.subConditions;
    }

    @Override
    public ColumnConditionType getType() {
        return ColumnConditionType.COMPOSITE_CONDITION;
    }

    @Override
    public ByteString serialize() {
        return ColumnConditionBuilder.buildCompositeCondition(this);
    }
}
