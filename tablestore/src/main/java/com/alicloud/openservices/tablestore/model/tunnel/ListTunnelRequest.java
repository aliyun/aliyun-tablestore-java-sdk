package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * ListTunnelRequest contains some necessary parameters for listing the Tunnels under a table, including the table name.
 */
public class ListTunnelRequest implements Request {
    /**
     * Table name.
     */
    private String tableName;

    /**
     * Initialize the ListTunnelRequest instance.
     * @param tableName The name of the table.
     */
    public ListTunnelRequest(String tableName) {
        setTableName(tableName);
    }

    /**
     * Get the table name.
     * @return The table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Set the table name.
     * @param tableName The name of the table.
     */
    public void setTableName(String tableName) {
        //Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
        //    "The table name should not be empty.");
        this.tableName = tableName;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_LIST_TUNNEL;
    }
}
