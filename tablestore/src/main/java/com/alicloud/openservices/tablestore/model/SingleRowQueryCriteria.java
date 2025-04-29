package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * Parameters required to query a single row of data from the TableStore table, supporting the following read behaviors:
 * <ul>
 * <li>Read a specific version of certain columns or all columns</li>
 * <li>Read all versions or the largest N versions within a version range for certain columns or all columns</li>
 * <li>Read the largest N versions of certain columns or all columns (N is at least 1 and at most MaxVersions)</li>
 * </ul>
 */
public class SingleRowQueryCriteria extends RowQueryCriteria implements IRow {

    private PrimaryKey primaryKey;

    /**
     * Used for in-row streaming read, to mark position and status information.
     */
    private OptionalValue<byte[]> token = new OptionalValue<byte[]>("Token");

    /**
     * Constructs a query condition for a table with the given name.
     *
     * @param tableName The name of the table to query
     */
    public SingleRowQueryCriteria(String tableName) {
        super(tableName);
    }

    /**
     * Constructs a query condition in the table with the given name.
     *
     * @param tableName The name of the table to query.
     * @param primaryKey The primary key of the row.
     */
    public SingleRowQueryCriteria(String tableName, PrimaryKey primaryKey) {
        super(tableName);
        setPrimaryKey(primaryKey);
    }

    /**
     * Set the primary key of the row.
     *
     * @param primaryKey The primary key of the row.
     */
    public void setPrimaryKey(PrimaryKey primaryKey) {
        Preconditions.checkArgument(primaryKey != null && !primaryKey.isEmpty(), "The row's primary key should not be null or empty.");
        this.primaryKey = primaryKey;
    }

    public byte[] getToken() {
        if (!this.token.isValueSet()) {
            throw new IllegalStateException("The value of token is not set.");
        }
        return token.getValue();
    }

    public void setToken(byte[] token) {
        if (token != null) {
            this.token.setValue(token);
        }
    }

    public boolean hasSetToken() {
        return this.token.isValueSet();
    }

    @Override
    public int compareTo(IRow row) {
        return this.primaryKey.compareTo(row.getPrimaryKey());
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        return this.primaryKey;
    }
}
