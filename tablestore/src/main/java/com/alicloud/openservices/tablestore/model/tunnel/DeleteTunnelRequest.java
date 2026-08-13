package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * DeleteTunnelRequest contains some necessary parameters for deleting a Tunnel, including the table name and Tunnel name.
 * Optionally, a Tunnel ID can be provided to identify the Tunnel directly.
 */
public class DeleteTunnelRequest implements Request {
    /**
     * Table name.
     */
    private String tableName;

    /**
     * Tunnel name.
     */
    private String tunnelName;

    /**
     * Tunnel ID (optional).
     */
    private String tunnelId;

    /**
     * Initializes an instance of DeleteTunnelRequest.
     *
     * @param tableName  The name of the table.
     * @param tunnelName The name of the tunnel.
     */
    public DeleteTunnelRequest(String tableName, String tunnelName) {
        this(tableName, tunnelName, null);
    }

    /**
     * Initializes an instance of DeleteTunnelRequest.
     *
     * @param tableName  The name of the table.
     * @param tunnelName The name of the tunnel.
     * @param tunnelId   The ID of the tunnel.
     */
    public DeleteTunnelRequest(String tableName, String tunnelName, String tunnelId) {
        setTableName(tableName);
        setTunnelName(tunnelName);
        setTunnelId(tunnelId);
    }

    /**
     * Get the table name.
     *
     * @return The table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Set the table name.
     *
     * @param tableName The name of the table.
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(),
            "The table name should not be empty.");
        this.tableName = tableName;
    }

    /**
     * Get the Tunnel name.
     *
     * @return The Tunnel name.
     */
    public String getTunnelName() {
        return tunnelName;
    }

    /**
     * Sets the Tunnel name.
     *
     * @param tunnelName The name of the Tunnel.
     */
    public void setTunnelName(String tunnelName) {
        Preconditions.checkArgument(tunnelName != null && !tunnelName.isEmpty(),
            "The tunnel name should not be empty.");
        this.tunnelName = tunnelName;
    }

    /**
     * Get the Tunnel ID.
     *
     * @return The Tunnel ID, or null if not set.
     */
    public String getTunnelId() {
        return tunnelId;
    }

    /**
     * Set the Tunnel ID (optional).
     *
     * @param tunnelId The ID of the Tunnel.
     */
    public void setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_TUNNEL;
    }
}
