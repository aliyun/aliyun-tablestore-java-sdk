package com.alicloud.openservices.tablestore.model.condition;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.google.protobuf.ByteString;

/**
 * 单行列值条件,用于TableStore的条件更新功能, 可以设置列值与设定值的大小关系条件.
 * <p>支持的比较关系包括：EQUAL(=), NOT_EQUAL(!=), GREATER_THAN(&gt;), GREATER_EQUAL(&gt;=), LESS_THAN(&lt;)以及LESS_EQUAL(&lt;=)。</p>
 * <p>由于TableStore一行的属性列不固定，有可能存在条件中的列在该行不存在的情况，这时{@link SingleColumnValueCondition#passIfMissing}参数控制在这种情况下条件是否通过。</p>
 * 如果设置{@link SingleColumnValueCondition#passIfMissing}为true，则若列在该行中不存在，则条件通过；
 * 如果设置{@link SingleColumnValueCondition#passIfMissing}为false，则若列在该行中不存在，则条件不通过。
 * 默认值为true。
 * <p>由于TableStore的属性列可能有多个版本，有可能存在该列的一个版本的值与给定值匹配但是另一个版本的值不匹配的情况，
 * 这时{@link SingleColumnValueCondition#latestVersionsOnly}参数控制在这种情况下对该行的过滤结果。
 * 如果设置{@link SingleColumnValueCondition#latestVersionsOnly}为true，则只会对最新版本的值进行比较, 默认值为true.
 * 需要注意的是, 如果{@link SingleColumnValueCondition#latestVersionsOnly}为false, 会对该列的所有版本(最新的max_versions个)进行比较，只要有一个版本的值匹配就认为条件成立。</p>
 */
public class SingleColumnValueCondition implements ColumnCondition {

    public enum CompareOperator {
        EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL;
    }

    private CompareOperator operator;
    private String columnName;
    private ColumnValue columnValue;
    private boolean passIfMissing = true;
    private boolean latestVersionsOnly = true;

    /**
     * 构造函数。
     *
     * @param columnName 列的名称
     * @param operator 比较函数
     * @param columnValue 列的值
     */
    public SingleColumnValueCondition(String columnName, final CompareOperator operator, final ColumnValue columnValue) {
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
    public SingleColumnValueCondition setOperator(CompareOperator operator) {
        Preconditions.checkNotNull(operator, "The operator should not be null.");
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
    public SingleColumnValueCondition setColumnName(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "The name of column should not be null or empty.");
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
    public SingleColumnValueCondition setColumnValue(ColumnValue columnValue) {
        Preconditions.checkNotNull(columnValue, "The value of column should not be null.");
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
     * <p>由于TableStore一行的属性列不固定，有可能存在条件中的列在该行不存在的情况，这时{@link SingleColumnValueCondition#passIfMissing}参数控制在这种情况下条件是否通过。</p>
     * <p>如果设置{@link SingleColumnValueCondition#passIfMissing}为true，则若列在该行中不存在，则条件通过；</p>
     * <p>如果设置{@link SingleColumnValueCondition#passIfMissing}为false，则若列在该行中不存在，则条件不通过。</p>
     * <p>默认值为true。</p>
     *
     * @param passIfMissing
     * @return this for chain invocation
     */
    public SingleColumnValueCondition setPassIfMissing(boolean passIfMissing) {
        this.passIfMissing = passIfMissing;
        return this;
    }

    /**
     * 返回设置的latestVersionsOnly的值。
     *
     * @return latestVersionsOnly的值。
     */
    public boolean isLatestVersionsOnly() {
        return latestVersionsOnly;
    }

    /**
     * 设置latestVersionsOnly。
     * <p>由于TableStore的属性列可能有多个版本，有可能存在该列的一个版本的值与给定值匹配但是另一个版本的值不匹配的情况，</p>
     * <p>这时{@link SingleColumnValueCondition#latestVersionsOnly}参数控制在这种情况下对该行的过滤结果。</p>
     * <p>如果设置{@link SingleColumnValueCondition#latestVersionsOnly}为true，则只会对最新版本的值进行比较, 默认值为true.</p>
     * <p>需要注意的是, 如果{@link SingleColumnValueCondition#latestVersionsOnly}为false, 会对该列的所有版本(最新的max_versions个)进行比较，只要有一个版本的值匹配就认为条件成立。</p>
     *
     * @param latestVersionsOnly
     * @return
     */
    public SingleColumnValueCondition setLatestVersionsOnly(boolean latestVersionsOnly) {
        this.latestVersionsOnly = latestVersionsOnly;
        return this;
    }

    @Override
    public ColumnConditionType getConditionType() {
        return ColumnConditionType.SINGLE_COLUMN_VALUE_CONDITION;
    }

    public SingleColumnValueFilter toFilter() {
        SingleColumnValueFilter.CompareOperator filterOperator;
        switch (operator) {
            case EQUAL:
                filterOperator = SingleColumnValueFilter.CompareOperator.EQUAL;
                break;
            case NOT_EQUAL:
                filterOperator = SingleColumnValueFilter.CompareOperator.NOT_EQUAL;
                break;
            case GREATER_THAN:
                filterOperator = SingleColumnValueFilter.CompareOperator.GREATER_THAN;
                break;
            case GREATER_EQUAL:
                filterOperator = SingleColumnValueFilter.CompareOperator.GREATER_EQUAL;
                break;
            case LESS_THAN:
                filterOperator = SingleColumnValueFilter.CompareOperator.LESS_THAN;
                break;
            case LESS_EQUAL:
                filterOperator = SingleColumnValueFilter.CompareOperator.LESS_EQUAL;
                break;
            default:
                throw new ClientException("Unknown operator: " + operator.name());
        }

        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(columnName, filterOperator, columnValue);
        singleColumnValueFilter.setLatestVersionsOnly(latestVersionsOnly);
        singleColumnValueFilter.setPassIfMissing(passIfMissing);
        return singleColumnValueFilter;
    }

    @Override
    public ByteString serialize() {
        return OTSProtocolBuilder.buildSingleColumnValueFilter(toFilter());
    }
}
