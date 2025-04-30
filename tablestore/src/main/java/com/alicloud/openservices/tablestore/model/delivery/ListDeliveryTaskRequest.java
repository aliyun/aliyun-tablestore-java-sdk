package com.alicloud.openservices.tablestore.model.delivery;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

public class ListDeliveryTaskRequest implements Request {

    /**
     * Table name
     */
    private String tableName;

    /**
     * Initialize the ListDeliveryTaskRequest instance
     *
     * @param tableName Table name
     */
    public ListDeliveryTaskRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * Get the table name
     * @return table name
     */
    public String getTableName() { return tableName; }

    /**
     * Set the table name
     * @param tableName The name of the table
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "tableName should not be null or empty");
        this.tableName = tableName;
    }

    @Override
    public String getOperationName() { return OperationNames.OP_LIST_DELIVERY_TASK; }
}
