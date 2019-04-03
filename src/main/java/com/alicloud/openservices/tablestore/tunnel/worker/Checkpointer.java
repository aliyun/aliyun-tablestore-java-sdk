package com.alicloud.openservices.tablestore.tunnel.worker;

import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.TunnelClientInterface;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointResponse;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alicloud.openservices.tablestore.core.ErrorCode.CLIENT_ERROR;
import static com.alicloud.openservices.tablestore.core.ErrorCode.SEQUENCE_NUMBER_NOT_MATCH;
import static com.alicloud.openservices.tablestore.core.protocol.ResponseFactory.FINISH_TAG;

/**
 * Checkpoint记录器，用于Channel(向服务器)记录数据消费位点。
 */
public class Checkpointer implements ICheckpointer {
    private static final Logger LOG = LoggerFactory.getLogger(Checkpointer.class);

    private TunnelClientInterface client;
    private String tunnelId;
    private String clientId;
    private String channelId;
    private long sequenceNumber;

    public Checkpointer(TunnelClientInterface client, String tunnelId, String clientId,
                        String channelId, long sequenceNumber) {
        this.client = client;
        this.tunnelId = tunnelId;
        this.clientId = clientId;
        this.channelId = channelId;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public void checkpoint(String token) {
        try {
            if (token == null) {
                token = FINISH_TAG;
            }
            client.checkpoint(new CheckpointRequest(tunnelId, clientId, channelId, token, sequenceNumber));
            this.sequenceNumber += 1;
            LOG.info("Finish do checkpoint,token:{}, checkpointer: {}", token, this);
        } catch (TableStoreException te) {
            // 当发生序列号冲突时，从服务端读取sequenceNumber进行本机内存的更新。
            if (te.getErrorCode().equals(SEQUENCE_NUMBER_NOT_MATCH)) {
                try {
                    GetCheckpointResponse getResp = client.getCheckpoint(
                        new GetCheckpointRequest(tunnelId, clientId, channelId));
                    this.sequenceNumber = getResp.getSequenceNumber() + 1;
                } catch (Exception ge) {
                    String errorMsg = String.format("Checkpoint failed %s and check sequence failed %s", te, ge);
                    LOG.warn(errorMsg);
                    throw new TableStoreException(errorMsg, CLIENT_ERROR);
                }
            }
        } catch (Exception e) {
            LOG.warn("Checkpoint occurs error, detail: {}", e.toString());
        }
    }

    public TunnelClientInterface getClient() {
        return client;
    }

    public void setClient(TunnelClientInterface client) {
        this.client = client;
    }

    public String getTunnelId() {
        return tunnelId;
    }

    public void setTunnelId(String tunnelId) {
        this.tunnelId = tunnelId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[TunnelId: ").append(tunnelId).append(", ClientId: ").append(clientId)
            .append(", ChannelId: ").append(channelId).append(", SequenceNumber: ").append(sequenceNumber).append("]");
        return sb.toString();
    }
}
