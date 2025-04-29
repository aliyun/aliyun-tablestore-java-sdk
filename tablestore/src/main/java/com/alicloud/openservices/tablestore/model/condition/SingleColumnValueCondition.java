package com.alicloud.openservices.tablestore.model.condition;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * Single row column value condition, used for the conditional update feature of TableStore. 
 * It allows setting conditions based on the size relationship between a column value and a specified value.
 * <p>Supported comparison relationships include: EQUAL(=), NOT_EQUAL(!=), GREATER_THAN(&gt;), GREATER_EQUAL(&gt;=), LESS_THAN(&lt;), and LESS_EQUAL(&lt;=).</p>
 * <p>Since the attribute columns in a single row of TableStore are not fixed, there may be cases where the column in the condition does not exist in that row. 
 * In such cases, the {@link SingleColumnValueCondition#passIfMissing} parameter controls whether the condition passes.</p>
 * If {@link SingleColumnValueCondition#passIfMissing} is set to true, the condition passes if the column does not exist in that row;
 * If {@link SingleColumnValueCondition#passIfMissing} is set to false, the condition fails if the column does not exist in that row.
 * The default value is true.
 * <p>Since TableStore's attribute columns may have multiple versions, there might be cases where one version of the column matches the given value while another does not.
 * In such cases, the {@link SingleColumnValueCondition#latestVersionsOnly} parameter controls the filtering result for that row.
 * If {@link SingleColumnValueCondition#latestVersionsOnly} is set to true, only the latest version of the value will be compared, with the default value being true.
 * It should be noted that if {@link SingleColumnValueCondition#latestVersionsOnly} is set to false, all versions (the latest max_versions) of the column will be compared, 
 * and the condition is considered met if any version matches.</p>
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
     * Constructor.
     *
     * @param columnName The name of the column
     * @param operator The comparison function
     * @param columnValue The value of the column
     */
    public SingleColumnValueCondition(String columnName, final CompareOperator operator, final ColumnValue columnValue) {
        setColumnName(columnName);
        setOperator(operator);
        setColumnValue(columnValue);
    }

    /**
     * Get the comparison operator for the column.
     *
     * @return The comparison operator.
     */
    public CompareOperator getOperator() {
        return operator;
    }

    /**
     * Set the column comparison operator.
     *
     * @param operator The comparison operator
     * @return this for chain invocation
     */
    public SingleColumnValueCondition setOperator(CompareOperator operator) {
        Preconditions.checkNotNull(operator, "The operator should not be null.");
        this.operator = operator;
        return this;
    }

    /**
     * Get the column name.
     *
     * @return The name of the column.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Sets the name of the column.
     *
     * @param columnName the name of the column
     * @return this for chain invocation
     */
    public SingleColumnValueCondition setColumnName(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "The name of column should not be null or empty.");
        this.columnName = columnName;
        return this;
    }

    /**
     * Get the comparison value of the column.
     *
     * @return The comparison value of the column.
     */
    public ColumnValue getColumnValue() {
        return columnValue;
    }

    /**
     * Sets the comparison value for the column.
     *
     * @param columnValue The comparison value for the column.
     * @return this for chain invocation
     */
    public SingleColumnValueCondition setColumnValue(ColumnValue columnValue) {
        Preconditions.checkNotNull(columnValue, "The value of column should not be null.");
        this.columnValue = columnValue;
        return this;
    }

    /**
     * Returns the value of passIfMissing that has been set.
     *
     * @return The value of passIfMissing.
     */
    public boolean isPassIfMissing() {
        return passIfMissing;
    }

    /**
     * Set passIfMissing.
     * <p>Since the property columns of a TableStore row are not fixed, there may be cases where a column in the condition does not exist in that row. In this case, the {@link SingleColumnValueCondition#passIfMissing} parameter controls whether the condition passes.</p>
     * <p>If {@link SingleColumnValueCondition#passIfMissing} is set to true, the condition will pass if the column does not exist in the row;</p>
     * <p>If {@link SingleColumnValueCondition#passIfMissing} is set to false, the condition will fail if the column does not exist in the row.</p>
     * <p>The default value is true.</p>
     *
     * @param passIfMissing
     * @return this for chain invocation
     */
    public SingleColumnValueCondition setPassIfMissing(boolean passIfMissing) {
        this.passIfMissing = passIfMissing;
        return this;
    }

    /**
     * Returns the value of the set latestVersionsOnly.
     *
     * @return The value of latestVersionsOnly.
     */
    public boolean isLatestVersionsOnly() {
        return latestVersionsOnly;
    }

    /**
     * Set latestVersionsOnly.
     * <p>Since TableStore property columns may have multiple versions, there might be cases where one version of the column's value matches the given value while another version does not,</p>
     * <p>in this case, the {@link SingleColumnValueCondition#latestVersionsOnly} parameter controls the filtering result for the row.</p>
     * <p>If {@link SingleColumnValueCondition#latestVersionsOnly} is set to true, only the latest version's value will be compared. The default value is true.</p>
     * <p>It should be noted that if {@link SingleColumnValueCondition#latestVersionsOnly} is false, all versions (the latest max_versions) of the column will be compared, and if any version's value matches, the condition is considered satisfied.</p>
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
