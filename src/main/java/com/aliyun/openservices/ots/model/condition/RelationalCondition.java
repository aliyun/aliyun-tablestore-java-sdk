package com.aliyun.openservices.ots.model.condition;

import com.aliyun.openservices.ots.model.ColumnValue;
import com.google.protobuf.ByteString;

public class RelationalCondition implements ColumnCondition {
    public static enum CompareOperator {
        EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL
    }

    private CompareOperator operator;
    private String columnName;
    private ColumnValue columnValue;
    private boolean passIfMissing = true;

    public RelationalCondition(
        String columnName, final CompareOperator operator, final ColumnValue columnValue) {
        setColumnName(columnName);
        setOperator(operator);
        setColumnValue(columnValue);
    }

    /**
     * 获取列的比较操作符。
     *
     * @return 比较操作符。
     */
    public CompareOperator getOperator() {
        return operator;
    }

    /**
     * 设置列的比较操作符。
     *
     * @param operator 比较操作符
     * @return this for chain invocation
     */
    public RelationalCondition setOperator(CompareOperator operator) {
        this.operator = operator;
        return this;
    }

    /**
     * 获取列名。
     *
     * @return 列的名称。
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * 设置列的名称。
     *
     * @param columnName 列的名称
     * @return this for chain invocation
     */
    public RelationalCondition setColumnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    /**
     * 获取列的对比值。
     *
     * @return 列的对比值。
     */
    public ColumnValue getColumnValue() {
        return columnValue;
    }

    /**
     * 设置列的对比值。
     *
     * @param columnValue 列的对比值。
     * @return this for chain invocation
     */
    public RelationalCondition setColumnValue(ColumnValue columnValue) {
        this.columnValue = columnValue;
        return this;
    }

    /**
     * 返回设置的passIfMissing的值。
     *
     * @return passIfMissing的值。
     */
    public boolean isPassIfMissing() {
        return passIfMissing;
    }

    /**
     * 设置passIfMissing。
     * <p/>于OTS一行的属性列不固定，有可能存在有condition条件的列在该行不存在的情况，这时{@link RelationalCondition#passIfMissing}参数控制在这种情况下对该行的检查结果。
     * <p/>如果设置{@link RelationalCondition#passIfMissing}为true，则若列在该行中不存在，则检查条件通过。
     * <p/>如果设置{@link RelationalCondition#passIfMissing}为false，则若列在该行中不存在，则检查条件失败。
     * <p/>默认值为true。
     *
     * @param passIfMissing
     * @return this for chain invocation
     */
    public RelationalCondition setPassIfMissing(boolean passIfMissing) {
        this.passIfMissing = passIfMissing;
        return this;
    }

    @Override
    public ColumnConditionType getType() {
        return ColumnConditionType.RELATIONAL_CONDITION;
    }

    @Override
    public ByteString serialize() {
        return ColumnConditionBuilder.buildRelationalCondition(this);
    }
}
