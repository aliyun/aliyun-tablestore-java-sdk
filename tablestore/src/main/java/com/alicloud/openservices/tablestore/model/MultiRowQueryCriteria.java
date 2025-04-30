package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * The parameters required to query multiple rows of data from the TableStore table, which can support the following several reading behaviors:
 * <ul>
 * <li>Read a specific version of certain columns or all columns</li>
 * <li>Read all versions or the latest N versions within a version range for certain columns or all columns</li>
 * <li>Read the latest N versions for certain columns or all columns (N is at least 1 and at most MaxVersions)</li>
 * </ul>
 * <p>Note: The read parameters cannot be set separately for each row; the same query parameters must be used for multiple rows.</p>
 */
public class MultiRowQueryCriteria extends RowQueryCriteria {

    private List<PrimaryKey> rowKeys;
    private List<byte[]> tokens;

    public MultiRowQueryCriteria(String tableName) {
        super(tableName);
        rowKeys = new ArrayList<PrimaryKey>();
        tokens = new ArrayList<byte[]>();
    }

    /**
     * Insert the primary key of the row to be queried into the multi-row query condition.
     *
     * @param primaryKey The primary key of the row to be queried.
     * @return this (for invocation chain)
     */
    public MultiRowQueryCriteria addRow(PrimaryKey primaryKey) {
        Preconditions.checkArgument(primaryKey != null && !primaryKey.isEmpty(), "The primary key added should not be null.");
        this.rowKeys.add(primaryKey);
        this.tokens.add(new byte[0]);
        return this;
    }

    /**
     * Insert the primary key of the row to be queried into the multi-row query condition.
     *
     * @param primaryKey The primary key of the row to be queried.
     * @return this (for invocation chain)
     */
    public MultiRowQueryCriteria addRow(PrimaryKey primaryKey, byte[] token) {
        Preconditions.checkArgument(primaryKey != null && !primaryKey.isEmpty(), "The primary key added should not be null.");
        this.rowKeys.add(primaryKey);
        this.tokens.add(token);
        return this;
    }

    /**
     * Get the primary key of the row to be queried in this table.
     *
     * @return The primary keys of all rows.
     */
    public List<PrimaryKey> getRowKeys() {
        return rowKeys;
    }

    /**
     * Set the primary keys of all rows to be queried in this table.
     *
     * @param primaryKeys The primary keys of all rows.
     */
    public void setRowKeys(List<PrimaryKey> primaryKeys) {
        Preconditions.checkArgument(primaryKeys != null && !primaryKeys.isEmpty(), "The rows to get should not be null or empty.");
        clear();
        rowKeys.addAll(primaryKeys);
        for (int i = 0; i < primaryKeys.size(); i++) {
            tokens.add(new byte[0]);
        }
    }

    public List<byte[]> getTokens() {
        return tokens;
    }

    /**
     * Get the primary key of a certain row.
     * <p>If the row index does not exist, return null.</p>
     *
     * @param index The index of the row
     * @return If the row exists, return the primary key of the row; otherwise, return null
     */
    public PrimaryKey get(int index) {
        Preconditions.checkArgument(index >= 0, "The index should not be negative.");

        if (rowKeys == null || rowKeys.isEmpty()) {
            return null;
        }

        if (index >= rowKeys.size()) {
            return null;
        }
        return rowKeys.get(index);
    }

    /**
     * Clear all rows to be queried.
     */
    public void clear() {
        this.rowKeys.clear();
    }

    /**
     * Get the number of rows to be queried.
     *
     * @return The number of rows.
     */
    public int size() {
        return rowKeys.size();
    }

    public boolean isEmpty() {
        return rowKeys.isEmpty();
    }

    public MultiRowQueryCriteria cloneWithoutRowKeys() {
        MultiRowQueryCriteria newCriteria = new MultiRowQueryCriteria(this.getTableName());
        this.copyTo(newCriteria);
        return newCriteria;
    }
}
