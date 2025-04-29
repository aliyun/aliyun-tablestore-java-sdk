package com.alicloud.openservices.tablestore.model.sql;

/**
 * Represents the data return collection of an SQL table
 */
public interface SQLResultSet {

    /**
     * Returns the Schema of the dataset
     *
     * @return
     */
    SQLTableMeta getSQLTableMeta();

    /**
     * Whether there is next data.
     * @return
     */
    boolean hasNext();

    /**
     * Return the next piece of data
     * @return
     */
    SQLRow next();

    /**
     * Return the total number of rows
     * @return
     */
    long rowCount();

    /**
     * Jump to the row at rowIndex
     *
     * @param rowIndex Column cursor
     * @return Returns true if the jump is successful, otherwise (e.g., out of bounds), returns false
     */
    boolean absolute(int rowIndex);

}
