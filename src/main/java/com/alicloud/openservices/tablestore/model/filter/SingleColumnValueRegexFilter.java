package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.Filter;

import com.google.protobuf.ByteString;

/**
 * TableStore查询操作时使用的过滤器，SingleColumnValueRegexFilter扩展子SingleColumnValueFilter，列值正则匹配之后，再比较。
 */
public class SingleColumnValueRegexFilter extends ColumnValueFilter {
    public enum CompareOperator {
        EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL, EXIST, NOT_EXIST;
    }

    private CompareOperator operator;
    private String columnName;
    private ColumnValue columnValue;
    private boolean latestVersionsOnly = true;
    private OptionalValue<RegexRule> regexRule = new OptionalValue<RegexRule>("RegexRule");

    /**
     * 构造函数。
     *
     * @param columnName 列的名称
     * @param rule 正则规则
     * @param operator 比较函数, 操作符限定在EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL
     * @param columnValue 列的值
     *
     * 说明：若filter期望的列不存在时，则该行被过滤掉
     */
    public SingleColumnValueRegexFilter(String columnName, final RegexRule rule, final CompareOperator operator, final ColumnValue columnValue) {
        if (operator == CompareOperator.EXIST || operator == CompareOperator.NOT_EXIST) {
            throw new ClientException("operator " + operator.name() + " should not use in this construct function");
        }
        setColumnName(columnName);
        setOperator(operator);
        setColumnValue(columnValue);
        setRegexRule(rule);
    }

    /**
     * 构造函数。
     *
     * @param columnName 列的名称
     * @param operator 比较函数，操作符限定在EXIST, NOT_EXIST
     */
    public SingleColumnValueRegexFilter(String columnName, final RegexRule rule, final CompareOperator operator) {
        if (operator != CompareOperator.EXIST && operator != CompareOperator.NOT_EXIST) {
            throw new ClientException("operator " + operator.name() + " should not use in this construct function");
        }
        setColumnName(columnName);
        setOperator(operator);
        setColumnValue(ColumnValue.fromLong(0));
        setRegexRule(rule);
    }

    /**
     * 空regex构造函数。
     *
     * @param columnName 列的名称
     * @param operator 比较函数, 操作符限定在EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL
     * @param columnValue 列的值
     *
     * 说明：若filter期望的列不存在时，则该行被过滤掉
     */
    public SingleColumnValueRegexFilter(String columnName, final CompareOperator operator, final ColumnValue columnValue) {
        if (operator == CompareOperator.EXIST || operator == CompareOperator.NOT_EXIST) {
            throw new ClientException("operator " + operator.name() + " should not use in this construct function");
        }
        setColumnName(columnName);
        setOperator(operator);
        setColumnValue(columnValue);
    }

    /**
     * 空regex构造函数。
     *
     * @param columnName 列的名称
     * @param operator 比较函数，操作符限定在EXIST, NOT_EXIST
     */
    public SingleColumnValueRegexFilter(String columnName, final CompareOperator operator) {
        if (operator != CompareOperator.EXIST && operator != CompareOperator.NOT_EXIST) {
            throw new ClientException("operator " + operator.name() + " should not use in this construct function");
        }
        setColumnName(columnName);
        setOperator(operator);
        setColumnValue(ColumnValue.fromLong(0));
    }

    /**
     * 设置列的比较操作符。
     *
     * @param operator 比较操作符
     * @return this for chain invocation
     */
    private SingleColumnValueRegexFilter setOperator(CompareOperator operator) {
        Preconditions.checkNotNull(operator, "The operator should not be null.");
        this.operator = operator;
        return this;
    }

    public SingleColumnValueRegexFilter.CompareOperator getOperator() {
        return operator;
    }

    /**
     * 设置列的名称。
     *
     * @param columnName 列的名称
     * @return this for chain invocation
     */
    private SingleColumnValueRegexFilter setColumnName(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "The name of column should not be null or empty.");
        this.columnName = columnName;
        return this;
    }

    public String getColumnName() {
        return columnName;
    }

    /**
     * 设置列的对比值。
     *
     * @param columnValue 列的对比值。
     * @return this for chain invocation
     */
    private SingleColumnValueRegexFilter setColumnValue(ColumnValue columnValue) {
        Preconditions.checkNotNull(columnValue, "The value of column should not be null.");
        this.columnValue = columnValue;
        return this;
    }

    public ColumnValue getColumnValue() {
        return columnValue;
    }

    /**
     * 设置正则规则。
     *
     * @param rule 正则表达式, cast type。
     * @return this for chain invocation
     */
    private SingleColumnValueRegexFilter setRegexRule(RegexRule rule) {
        this.regexRule.setValue(rule);
        return this;
    }

    public boolean hasRegexRule() {
        return this.regexRule.isValueSet();
    }

    public RegexRule getRegexRule() {
        return this.regexRule.getValue();
    }

    /**
     * 设置多版本filter策略。
     *
     * @param latestVersionsOnly, 为true，则只会对最新版本的值进行比较，否则会对该列的所有版本(最新的max_versions个)进行比较
     * @return this for chain invocation
     */
    public SingleColumnValueRegexFilter setLatestVersionsOnly(boolean latestVersionsOnly) {
        this.latestVersionsOnly = latestVersionsOnly;
        return this;
    }

    public boolean getLatestVersionsOnly() {
        return latestVersionsOnly;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.SINGLE_COLUMN_VALUE_FILTER;
    }

    @Override
    public ByteString serialize() {
        return OTSProtocolBuilder.buildSingleColumnValueRegexFilter(this);
    }
}
