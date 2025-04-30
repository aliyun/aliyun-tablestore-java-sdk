package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * Basic structure for single-row data change operations.
 * <p>If it is a PutRow operation, refer to {@link RowPutChange}.</p>
 * <p>If it is an UpdateRow operation, refer to {@link RowUpdateChange}.</p>
 * <p>If it is a DeleteRow operation, refer to {@link RowDeleteChange}.</p>
 */
public abstract class RowChange implements IRow, Measurable {
    /**
     * The name of the table.
     */
    private String tableName;
    
    /**
     * The primary key of the table.
     */
    private PrimaryKey primaryKey;
    
    /**
     * Judgment condition.
     */
    private Condition condition;
    
    /**
     * The return data type, the default is not to return.
     */
    private ReturnType returnType;

    /**
     * Specify the column names for which the modified values need to be returned. Supported modification type: atomic add.
     */
    private Set<String> returnColumnNames = new HashSet<String>();

    /**
     * Constructor.
     * Internal use.
     * <p>The table name cannot be null or empty.</p>
     * <p>The primary key of the row cannot be null or empty.</p>
     *
     * @param tableName The name of the table
     * @param primaryKey The primary key of the table
     */
    public RowChange(String tableName, PrimaryKey primaryKey){
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");
        Preconditions.checkArgument(primaryKey != null && !primaryKey.isEmpty(), "The primary key of row should not be null or empty.");
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.condition = new Condition();
        this.returnType = ReturnType.RT_NONE;
    }
    
    /**
     * Constructor.
     * Internal use
     * <p>The table name cannot be null or empty.</p>
     *
     * @param tableName the name of the table
     */
    public RowChange(String tableName) {
    	Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");
        this.tableName = tableName;
        this.condition = new Condition();
        this.returnType = ReturnType.RT_NONE;
    }
    
    /**
     * Set the name of the table.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
    /**
     * Get the name of the table.
     *
     * @return the name of the table
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Add the name and value of the primary key (Primary Key) column.
     * @param primaryKey The primary key of the row.
     */
    public void setPrimaryKey(PrimaryKey primaryKey){
        Preconditions.checkNotNull(primaryKey, "primaryKey");

        this.primaryKey = primaryKey;
    }

    /**
     * Get the primary key of this row.
     *
     * @return the primary key of the row
     */
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Get the judgment condition.
     * @return The judgment condition.
     */
    public Condition getCondition() {
        return condition;
    }

    /**
     * Set the judgment condition.
     * @param condition The judgment condition.
     */
    public void setCondition(Condition condition) {
        this.condition = condition;
    }

    @Override
    public int compareTo(IRow row) {
        return this.primaryKey.compareTo(row.getPrimaryKey());
    }

    public ReturnType getReturnType() {
        return returnType;
    }

    public void setReturnType(ReturnType returnType) {
        this.returnType = returnType;
    }

    public void addReturnColumn(String columnName) {
        Preconditions.checkArgument(columnName != null && !columnName.isEmpty(), "Column's name should not be null or empty.");
        this.returnColumnNames.add(columnName);
    }

    public Set<String> getReturnColumnNames() {
        return Collections.unmodifiableSet(returnColumnNames);
    }

}
