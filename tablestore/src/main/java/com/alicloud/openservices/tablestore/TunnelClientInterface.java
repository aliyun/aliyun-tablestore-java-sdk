package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.ListTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ShutdownTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ShutdownTunnelResponse;

public interface TunnelClientInterface {
    /**
     * Create a Tunnel.
     * @param request Parameters required for creating a Tunnel, see {@link CreateTunnelRequest}
     * @return The result returned after creating the Tunnel, see {@link CreateTunnelResponse}
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    CreateTunnelResponse createTunnel(CreateTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * Get the Tunnel information for a specific table.
     * @param request Parameters required to list Tunnels under a table, see {@link ListTunnelRequest}
     * @return A list of detailed Tunnel information, see {@link ListTunnelResponse}
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    ListTunnelResponse listTunnel(ListTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * Get the specific information under a certain Tunnel.
     * @param request Parameters required to describe the detailed information of a certain Tunnel, see {@link DescribeTunnelRequest}
     * @return Specific information under the Tunnel, including Channel information and RPO information, etc., see {@link DescribeTunnelResponse}
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException The response result of the request is invalid, or a network exception occurred
     */
    DescribeTunnelResponse describeTunnel(DescribeTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * Delete a Tunnel.
     * @param request Parameters required to delete a Tunnel, see {@link DeleteTunnelRequest}
     * @return The result of deleting the Tunnel, see {@link DeleteTunnelResponse}
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid request response or network exception encountered
     */
    DeleteTunnelResponse deleteTunnel(DeleteTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * Advanced interface: Not recommended for direct use. If there is no special requirement, please prioritize using the TunnelWorker automated data processing framework.
     * Assigns a client identifier (ClientId) to a specified Tunnel and can also transmit some client parameters to the Master, such as client heartbeat timeout and client type, etc.
     * @param request Parameters required to connect to a Tunnel, see {@link ConnectTunnelRequest} for details
     * @return The result of connecting to the Tunnel, see {@link ConnectTunnelResponse} for details
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    ConnectTunnelResponse connectTunnel(ConnectTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * Advanced Interface: Not recommended for direct use. If there is no special requirement, please prioritize using the TunnelWorker automated data processing framework.
     * The function of the Heartbeat operation is to (TunnelClient) maintain a heartbeat. It can also obtain consumable Channels and report some processing statuses.
     *      When the heartbeat times out, the TunnelClient is considered offline and needs to reconnect.
     * @param request Parameters required for detecting the heartbeat, see {@link HeartbeatRequest} for details.
     * @return The result of the heartbeat detection, see {@link HeartbeatResponse} for details.
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid return result of the request or network exception encountered
     */
    HeartbeatResponse heartbeat(HeartbeatRequest request)
        throws TableStoreException, ClientException;

    /**
     * Advanced Interface: Not recommended for direct use. If there is no special requirement, please prioritize using the TunnelWorker automated data processing framework.
     * Close a TunnelClient and disconnect from the Tunnel server.
     * @param request Parameters required to shut down a TunnelClient, see {@link ShutdownTunnelRequest}
     * @return The result of shutting down the TunnelClient, see {@link ShutdownTunnelResponse}
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response or network exception encountered during the request
     */
    ShutdownTunnelResponse shutdownTunnel(ShutdownTunnelRequest request)
        throws TableStoreException, ClientException;

    /**
     * Advanced Interface: Not recommended for direct use. If there is no special requirement, please prioritize using the TunnelWorker automated data processing framework.
     * Get the Checkpoint and the SequenceNumber corresponding to the Checkpoint that was last recorded for a specific Channel.
     * @param request Parameters required to get the Checkpoint of a specific Channel, see {@link GetCheckpointRequest}
     * @return The result of the Checkpoint last recorded for a specific Channel
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response from the request or network exception encountered
     */
    GetCheckpointResponse getCheckpoint(GetCheckpointRequest request)
        throws TableStoreException, ClientException;

    /**
     * Advanced Interface: Not recommended for direct use. If there is no special requirement, 
     *    please prioritize using the TunnelWorker automated data processing framework.
     * Read data from a specific Channel by specifying the Tunnel ID, Client ID, Channel ID, and starting Token value. 
     * During the first read, use the previously recorded Token (Checkpoint) to start reading, and subsequently use the returned NextToken to continue reading.
     * @param request Read data from a specific Channel, see details in {@link ReadRecordsRequest}
     * @return Data from a specific Channel (either full or incremental type data)
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException Invalid response result or network exception encountered during the request
     */
    ReadRecordsResponse readRecords(ReadRecordsRequest request)
        throws TableStoreException, ClientException;

    /**
     * Advanced Interface: Not recommended for direct use. If there is no special requirement, please prioritize using the TunnelWorker automated data processing framework.
     * Set the Checkpoint and the SequenceNumber corresponding to the Checkpoint for a specific Channel.
     * @param request Set the Checkpoint for a specific Channel
     * @return 
     * @throws TableStoreException Exception returned by the TableStore service
     * @throws ClientException The response result is invalid or a network exception occurred
     */
    CheckpointResponse checkpoint(CheckpointRequest request)
        throws TableStoreException, ClientException;

    /**
     * Release resources.
     * <p>Make sure to release resources after all requests have been executed. After releasing resources, no further requests can be sent, and ongoing requests may not return results.</p>
     */
    void shutdown();
}
