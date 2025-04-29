package com.alicloud.openservices.tablestore.timestream;

import com.alicloud.openservices.tablestore.timestream.model.AttributeIndexSchema;

import java.util.List;

public interface TimestreamDB {

    /**
     * Close the client and release resources.
     * <p>Make sure to release resources after all requests have been executed. After releasing resources, no further requests can be sent, and ongoing requests may not return results.</p>
     */
    public void close();

    /**
     * Create the meta table without creating indexes for attributes.
     */
    public void createMetaTable();

    /**
     * Create the meta table and create indexes for the specified attributes.
     * <p>Attributes must not be reserved fields (h, n, t, s).</p>
     */
    public void createMetaTable(List<AttributeIndexSchema> indexForAttributes);

    /**
     * Delete the meta table
     */
    public void deleteMetaTable();

    /**
     * Create a data table
     * @param tableName Name of the data table
     */
    public void createDataTable(String tableName);

    /**
     * Delete the table
     * @param tableName the name of the table
     */
    public void deleteDataTable(String tableName);

    /**
     * Get the operation object for the meta table
     * @return
     */
    public TimestreamMetaTable metaTable();

    /**
     * Get the operation object of the data table
     * @param tableName Name of the data table
     * @return
     */
    public TimestreamDataTable dataTable(String tableName);
}