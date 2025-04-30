package com.alicloud.openservices.tablestore.model.sql;

/**
 * Represents the data type of a SQL Statement.
 */
public enum SQLStatementType {
    /**
     * Query.
     */
    SQL_SELECT,

    /**
     * Create table.
     */
    SQL_CREATE_TABLE,

    /**
     * Query the table list.
     */
    SQL_SHOW_TABLE,

    /**
     * Query table format.
     */
    SQL_DESCRIBE_TABLE,

    /**
     * Delete the table.
     */
    SQL_DROP_TABLE,

    /**
     * Modify the table.
     */
    SQL_ALTER_TABLE,
}
