package com.alicloud.openservices.tablestore.model.condition;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.filter.CompositeColumnValueFilter;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * 组合列值条件, 用于TableStore的条件更新功能.
 * {@link CompositeColumnValueCondition}可以对{@link SingleColumnValueCondition}或{@link CompositeColumnValueCondition}进行逻辑条件组合, 有NOT、AND和OR三种逻辑关系条件，其中NOT和AND表示二元或多元的关系，NOT只表示一元的关系。
 * <p>逻辑关系通过构造函数{@link CompositeColumnValueCondition#CompositeColumnValueCondition(CompositeColumnValueCondition.LogicOperator)}的参数提供。</p>
 * <p>若逻辑关系为{@link CompositeColumnValueCondition.LogicOperator#NOT}，可以通过{@link CompositeColumnValueCondition#addCondition(ColumnCondition)}添加ColumnCondition，但是添加的ColumnCondition有且只有一个。</p>
 * <p>若逻辑关系为{@link CompositeColumnValueCondition.LogicOperator#AND}，可以通过{@link CompositeColumnValueCondition#addCondition(ColumnCondition)}添加ColumnCondition，添加的ColumnCondition必须大于等于两个。</p>
 * <p>若逻辑关系为{@link CompositeColumnValueCondition.LogicOperator#OR}，可以通过{@link CompositeColumnValueCondition#addCondition(ColumnCondition)}添加ColumnCondition，但是添加的ColumnCondition必须大于等于两个。</p>
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
     * 增加逻辑关系组中的ColumnCondition。
     * <p>若逻辑关系为{@link CompositeColumnValueCondition.LogicOperator#NOT}，有且只能添加一个ColumnCondition。</p>
     * <p>若逻辑关系为{@link CompositeColumnValueCondition.LogicOperator#AND}，必须添加至少两个ColumnCondition。</p>
     * <p>若逻辑关系为{@link CompositeColumnValueCondition.LogicOperator#OR}，必须添加至少两个ColumnCondition。</p>
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
     * 清空逻辑关系组中的所有ColumnCondition。
     */
    public void clear() {
        this.conditions.clear();
    }

    /**
     * 查看当前设置的逻辑关系。
     *
     * @return 逻辑关系符号。
     */
    public LogicOperator getOperationType() {
        return this.type;
    }

    /**
     * 返回逻辑关系组中的所有ColumnCondition。
     *
     * @return 所有ColumnCondition。
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
