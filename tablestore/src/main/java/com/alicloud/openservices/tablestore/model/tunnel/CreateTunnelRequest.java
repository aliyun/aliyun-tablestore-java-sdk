package com.alicloud.openservices.tablestore.model.tunnel;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.Request;

/**
 * CreateTunnelRequest contains some necessary parameters for creating a new Tunnel, including table name, Tunnel name, and the type of Tunnel.
 */
public class CreateTunnelRequest implements Request {
    /**
     * Table name.
     */
    private String tableName;

    /**
     * Tunnel name.
     */
    private String tunnelName;

    /**
     * Tunnel type, currently supports three types: full, incremental, and full plus incremental.
     */
    private TunnelType tunnelType;

    /**
     * Tunnel time range configuration.
     */
    private StreamTunnelConfig streamTunnelConfig;

    /**
     * Initialize the CreateTunnelRequest instance.
     *
     * @param tableName  The name of the table.
     * @param tunnelName The name of the tunnel.
     * @param tunnelType The type of the tunnel.
     */
    public CreateTunnelRequest(String tableName, String tunnelName,
                               TunnelType tunnelType) {
        setTableName(tableName);
        setTunnelName(tunnelName);
        setTunnelType(tunnelType);
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
     * Get the Tunnel type.
     *
     * @return The Tunnel type.
     */
    public TunnelType getTunnelType() {
        return tunnelType;
    }

    /**
     * Set the Tunnel type.
     *
     * @param tunnelType The type of Tunnel.
     */
    public void setTunnelType(TunnelType tunnelType) {
        Preconditions.checkArgument(tunnelType != null,
            "The tunnel type should not be null.");
        this.tunnelType = tunnelType;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_TUNNEL;
    }

    public StreamTunnelConfig getStreamTunnelConfig() {
        return streamTunnelConfig;
    }

    public void setStreamTunnelConfig(StreamTunnelConfig streamTunnelConfig) {
        this.streamTunnelConfig = streamTunnelConfig;
    }
}
