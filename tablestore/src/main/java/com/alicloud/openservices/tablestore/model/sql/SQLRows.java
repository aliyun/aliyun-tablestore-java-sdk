package com.alicloud.openservices.tablestore.model.sql;

/**
 * Represents the dataset of SQL data storage
 */
public interface SQLRows {

    /**
     * Returns the Schema of the dataset
     *
     * @return Schema of the dataset
     */
    SQLTableMeta getSQLTableMeta();

    /**
     * Returns the total number of rows in the dataset
     *
     * @return total number of rows
     */
    long rowCount();

    /**
     * Returns the number of columns in the dataset
     *
     * @return total number of rows
     */
    long columnCount();

    /**
     * Query the data of a specific row and column by row cursor and column cursor
     *
     * @param rowIndex    row cursor
     * @param columnIndex column cursor
     * @return data
     */
    Object get(int rowIndex, int columnIndex);

}
