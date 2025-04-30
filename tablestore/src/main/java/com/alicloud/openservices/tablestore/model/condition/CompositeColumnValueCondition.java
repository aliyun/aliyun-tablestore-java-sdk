package com.alicloud.openservices.tablestore.model.condition;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * Composite column value condition, used for the conditional update feature of TableStore.
 * {@link CompositeColumnValueCondition} can combine logical conditions of {@link SingleColumnValueCondition} or {@link CompositeColumnValueCondition}, with three types of logical relationships: NOT, AND, and OR. Among them, NOT and AND represent binary or multi-element relationships, while NOT only represents a unary relationship.
 * <p>The logical relationship is provided by the parameter in the constructor {@link CompositeColumnValueCondition#CompositeColumnValueCondition(CompositeColumnValueCondition.LogicOperator)}.</p>
 * <p>If the logical relationship is {@link CompositeColumnValueCondition.LogicOperator#NOT}, you can add a ColumnCondition via {@link CompositeColumnValueCondition#addCondition(ColumnCondition)}, but only one ColumnCondition can be added.</p>
 * <p>If the logical relationship is {@link CompositeColumnValueCondition.LogicOperator#AND}, you can add a ColumnCondition via {@link CompositeColumnValueCondition#addCondition(ColumnCondition)}. The number of added ColumnConditions must be greater than or equal to two.</p>
 * <p>If the logical relationship is {@link CompositeColumnValueCondition.LogicOperator#OR}, you can add a ColumnCondition via {@link CompositeColumnValueCondition#addCondition(ColumnCondition)}, but the number of added ColumnConditions must be greater than or equal to two.</p>
 */
public class CompositeColumnValueCondition implements ColumnCondition {

    public enum LogicOperator {
        NOT, AND, OR;
    }

    private LogicOperator type;
    private List<ColumnCondition> conditions;

    public CompositeColumnValueCondition(LogicOperator loType) {
        Preconditions.checkNotNull(loType, "The operation type should not be null.");
        this.type = loType;
        this.conditions = new ArrayList<ColumnCondition>();
    }

    /**
     * Add a ColumnCondition to the logical relationship group.
     * <p>If the logical relationship is {@link CompositeColumnValueCondition.LogicOperator#NOT}, only one ColumnCondition can be added.</p>
     * <p>If the logical relationship is {@link CompositeColumnValueCondition.LogicOperator#AND}, at least two ColumnConditions must be added.</p>
     * <p>If the logical relationship is {@link CompositeColumnValueCondition.LogicOperator#OR}, at least two ColumnConditions must be added.</p>
     *
     * @param condition
     * @return
     */
    public CompositeColumnValueCondition addCondition(ColumnCondition condition) {
        Preconditions.checkNotNull(condition, "The condition should not be null.");
        this.conditions.add(condition);
        return this;
    }

    /**
     * Clear all ColumnConditions in the logical relationship group.
     */
    public void clear() {
        this.conditions.clear();
    }

    /**
     * View the currently set logical relationship.
     *
     * @return Logical relationship symbol.
     */
    public LogicOperator getOperationType() {
        return this.type;
    }

    /**
     * Returns all ColumnConditions in the logical relationship group.
     *
     * @return All ColumnConditions.
     */
    public List<ColumnCondition> getSubConditions() {
        return this.conditions;
    }

    @Override
    public ColumnConditionType getConditionType() {
        return ColumnConditionType.COMPOSITE_COLUMN_VALUE_CONDITION;
    }

    public CompositeColumnValueFilter toFilter() {
        CompositeColumnValueFilter.LogicOperator logicOperator;
        switch (type) {
            case NOT:
                logicOperator = CompositeColumnValueFilter.LogicOperator.NOT;
                break;
            case AND:
                logicOperator = CompositeColumnValueFilter.LogicOperator.AND;
                break;
            case OR:
                logicOperator = CompositeColumnValueFilter.LogicOperator.OR;
                break;
            default:
                throw new ClientException("Unknown logicOperator: " + type.name());
        }
        CompositeColumnValueFilter compositeColumnValueFilter = new CompositeColumnValueFilter(logicOperator);
        for (ColumnCondition condition : getSubConditions()) {
            if (condition instanceof SingleColumnValueCondition) {
                compositeColumnValueFilter.addFilter(((SingleColumnValueCondition) condition).toFilter());
            } else if (condition instanceof CompositeColumnValueCondition) {
                compositeColumnValueFilter.addFilter(((CompositeColumnValueCondition) condition).toFilter());
            } else {
                throw new ClientException("Unknown condition type: " + condition.getConditionType());
            }
        }
        return compositeColumnValueFilter;
    }

    @Override
    public ByteString serialize() {
        return OTSProtocolBuilder.buildCompositeColumnValueFilter(toFilter());
    }
}
