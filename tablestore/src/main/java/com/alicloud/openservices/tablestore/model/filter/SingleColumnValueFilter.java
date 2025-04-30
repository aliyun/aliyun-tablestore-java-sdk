package com.alicloud.openservices.tablestore.model.filter;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;

/**
 * TableStore filter used during query operations. SingleColumnValueFilter is used to represent the comparison relationship between a data column and its value within the queried row.
 * <p>Possible column-value comparison relationships include: EQUAL(=), NOT_EQUAL(!=), GREATER_THAN(&gt;), GREATER_EQUAL(&gt;=), LESS_THAN(&lt;), and LESS_EQUAL(&lt;=).</p>
 * <p>Since the attribute columns in a TableStore row are not fixed, it's possible that a column specified in the filter condition may not exist in that row. In such cases, the {@link SingleColumnValueFilter#passIfMissing} parameter controls the filtering result for that row.</p>
 * If {@link SingleColumnValueFilter#passIfMissing} is set to true, then if the column does not exist in the row, the row will be returned;
 * If {@link SingleColumnValueFilter#passIfMissing} is set to false, then if the column does not exist in the row, the row will not be returned.
 * The default value is true.
 * <p>Since TableStore attribute columns may have multiple versions, there might be cases where one version of the column matches the given value while another version does not.</p>
 * In such cases, the {@link SingleColumnValueFilter#latestVersionsOnly} parameter controls the filtering result for that row.
 * If {@link SingleColumnValueFilter#latestVersionsOnly} is set to true, only the latest version of the value will be compared. Otherwise, all versions (the latest max_versions) of the column will be compared,
 * and if any version matches, the condition is considered satisfied. The default value is true.
 */
public class SingleColumnValueFilter extends ColumnValueFilter {
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
    public SingleColumnValueFilter(String columnName, final CompareOperator operator, final ColumnValue columnValue) {
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
     * Sets the column comparison operator.
     *
     * @param operator The comparison operator
     * @return this for chain invocation
     */
    public SingleColumnValueFilter setOperator(CompareOperator operator) {
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
    public SingleColumnValueFilter setColumnName(String columnName) {
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
    public SingleColumnValueFilter setColumnValue(ColumnValue columnValue) {
        Preconditions.checkNotNull(columnValue, "The value of column should not be null.");
        this.columnValue = columnValue;
        return this;
    }

    /**
     * Returns the value of the set passIfMissing.
     *
     * @return The value of passIfMissing.
     */
    public boolean isPassIfMissing() {
        return passIfMissing;
    }

    /**
     * Set passIfMissing.
     * <p>Since the attribute columns of a row in TableStore are not fixed, there might be cases where the column with a filter condition does not exist in that row. In such cases, the {@link SingleColumnValueFilter#passIfMissing} parameter controls the filtering result for that row.</p>
     * <p>If {@link SingleColumnValueFilter#passIfMissing} is set to true, the row will be returned if the column does not exist in that row;</p>
     * <p>If {@link SingleColumnValueFilter#passIfMissing} is set to false, the row will not be returned if the column does not exist in that row.</p>
     * <p>The default value is true.</p>
     *
     * @param passIfMissing
     * @return this for chain invocation
     */
    public SingleColumnValueFilter setPassIfMissing(boolean passIfMissing) {
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
     * <p>Since property columns in TableStore may have multiple versions, there might be cases where one version's value matches the given value while another version's does not,</p>
     * at this point, the {@link SingleColumnValueFilter#latestVersionsOnly} parameter controls the filtering result for such cases.
     * <p>If {@link SingleColumnValueFilter#latestVersionsOnly} is set to true, only the latest version will be compared. Otherwise, all versions (the latest max_versions) of that column will be compared,</p>
     * <p>and if any version matches, the filter condition is considered met. The default value is true.</p>
     *
     * @param latestVersionsOnly
     * @return
     */
    public SingleColumnValueFilter setLatestVersionsOnly(boolean latestVersionsOnly) {
        this.latestVersionsOnly = latestVersionsOnly;
        return this;
    }

    public SingleColumnValueCondition toCondition() {
        SingleColumnValueCondition.CompareOperator filterOperator;
        switch (operator) {
            case EQUAL:
                filterOperator = SingleColumnValueCondition.CompareOperator.EQUAL;
                break;
            case NOT_EQUAL:
                filterOperator = SingleColumnValueCondition.CompareOperator.NOT_EQUAL;
                break;
            case GREATER_THAN:
                filterOperator = SingleColumnValueCondition.CompareOperator.GREATER_THAN;
                break;
            case GREATER_EQUAL:
                filterOperator = SingleColumnValueCondition.CompareOperator.GREATER_EQUAL;
                break;
            case LESS_THAN:
                filterOperator = SingleColumnValueCondition.CompareOperator.LESS_THAN;
                break;
            case LESS_EQUAL:
                filterOperator = SingleColumnValueCondition.CompareOperator.LESS_EQUAL;
                break;
            default:
                throw new ClientException("Unknown operator: " + operator.name());
        }

        SingleColumnValueCondition singleColumnValueCondition = new SingleColumnValueCondition(columnName, filterOperator, columnValue);
        singleColumnValueCondition.setLatestVersionsOnly(latestVersionsOnly);
        singleColumnValueCondition.setPassIfMissing(passIfMissing);
        return singleColumnValueCondition;
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.SINGLE_COLUMN_VALUE_FILTER;
    }

    @Override
    public ByteString serialize() {
        return OTSProtocolBuilder.buildSingleColumnValueFilter(this);
    }
}
