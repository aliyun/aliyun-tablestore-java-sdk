package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * UpdateTableRequest contains some necessary parameters for updating a table, including the table name, reserved throughput changes, and table configuration changes, etc.
 * <p>Users can use UpdateTable to separately change the reserved throughput, or separately change some configuration items of the table, or do both together.</p>
 */
public class UpdateTableRequest implements Request {

    /**
     * The name of the table.
     */
    private String tableName;

    /**
     * Change of table's reserved throughput.
     * You can separately change the read capacity unit or the write capacity unit.
     */
    private ReservedThroughput reservedThroughputForUpdate;

    /**
     * Table configuration parameter options.
     */
    private TableOptions tableOptionsForUpdate;

    /**
     * Table Stream configuration change.
     */
    private StreamSpecification streamSpecification;

    public UpdateTableRequest() {
    }

    public UpdateTableRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * Get the name of the table.
     *
     * @return The name of the table.
     */
    public String getTableName() {
        Preconditions.checkNotNull(tableName);
        return tableName;
    }

    /**
     * Set the name of the table
     *
     * @param tableName the name of the table
     */
    public void setTableName(String tableName) {
        Preconditions.checkNotNull(tableName, "tableName must not be null.");
        Preconditions.checkArgument(!tableName.isEmpty(),
            "The name of table must not be empty.");
        this.tableName = tableName;
    }

    /**
     * Get the reserved throughput change of the table.
     *
     * @return The reserved throughput change of the table.
     */
    public ReservedThroughput getReservedThroughputForUpdate() {
        return reservedThroughputForUpdate;
    }

    /**
     * Set the reserved throughput change for the table.
     *
     * @param reservedThroughputForUpdate The reserved throughput change for the table.
     */
    public void setReservedThroughputForUpdate(ReservedThroughput reservedThroughputForUpdate) {
        this.reservedThroughputForUpdate = reservedThroughputForUpdate;
    }

    /**
     * Get the parameter changes of the table.
     *
     * @return The parameter changes of the table.
     */
    public TableOptions getTableOptionsForUpdate() {
        return tableOptionsForUpdate;
    }

    /**
     * Set the parameter changes for the table.
     *
     * @param tableOptionsForUpdate The parameter changes for the table.
     */
    public void setTableOptionsForUpdate(TableOptions tableOptionsForUpdate) {
        this.tableOptionsForUpdate = tableOptionsForUpdate;
    }

    public String getOperationName() {
        return OperationNames.OP_UPDATE_TABLE;
    }

    /**
     * Get the Stream configuration changes of the table.
     *
     * @return The Stream configuration changes of the table.
     */
    public StreamSpecification getStreamSpecification() {
        return streamSpecification;
    }

    /**
     * Set the Stream configuration change for the table.
     *
     * @param streamSpecification The Stream configuration change for the table.
     */
    public void setStreamSpecification(StreamSpecification streamSpecification) {
        this.streamSpecification = streamSpecification;
    }
}
