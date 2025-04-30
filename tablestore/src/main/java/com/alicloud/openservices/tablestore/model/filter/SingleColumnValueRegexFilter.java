package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.Filter;

import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * The filter used in TableStore query operations. SingleColumnValueRegexFilter is an extension of SingleColumnValueFilter. After the column value is matched with a regular expression, a comparison is performed.
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
     * Constructor.
     *
     * @param columnName The name of the column
     * @param rule The regular expression rule
     * @param operator The comparison operator, limited to EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL
     * @param columnValue The value of the column
     *
     * Note: If the column expected by the filter does not exist, then the row will be filtered out.
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
     * Constructor.
     *
     * @param columnName The name of the column
     * @param operator The comparison function, operator is limited to EXIST, NOT_EXIST
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
     * Empty regex constructor.
     *
     * @param columnName the name of the column
     * @param operator the comparison function, operator limited to EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_EQUAL, LESS_THAN, LESS_EQUAL
     * @param columnValue the value of the column
     *
     * Note: If the expected column in the filter does not exist, then this row will be filtered out.
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
     * Empty regex constructor.
     *
     * @param columnName The name of the column
     * @param operator Comparison function, the operator is limited to EXIST, NOT_EXIST
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
     * Sets the comparison operator for the column.
     *
     * @param operator The comparison operator
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
     * Sets the name of the column.
     *
     * @param columnName the name of the column
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
     * Set the comparison value of the column.
     *
     * @param columnValue The comparison value of the column.
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
     * Set the regular expression rule.
     *
     * @param rule The regular expression, cast type.
     * @return this for chain invocation.
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
     * Set the multi-version filter strategy.
     *
     * @param latestVersionsOnly if true, only the latest version of the value will be compared; otherwise, all versions (the latest max_versions) of the column will be compared.
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
