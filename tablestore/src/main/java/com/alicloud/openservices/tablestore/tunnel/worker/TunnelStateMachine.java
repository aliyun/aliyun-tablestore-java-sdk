package com.alicloud.openservices.tablestore.tunnel.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alicloud.openservices.tablestore.TunnelClientInterface;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TunnelStateMachine is used for maintaining and updating the states of Channel and ChannelConnect (including local memory and interaction with the Tunnel server).
 * There are five states for Channels: WAIT, OPEN, CLOSING, CLOSE, TERMINATED. The descriptions of Channel states are as follows:
 * 1. WAIT: Waiting state, when a Tunnel is created, the Tunnel server will pre-create a batch of Channels in the WAIT state to be allocated to Tunnel clients.
 * 2. OPEN: Active state, when the TunnelClient calls the heartbeat interface, the Tunnel server will allocate Channels to the TunnelClient and set their states to OPEN.
 * 3. CLOSING: Closing state, when a new TunnelClient joins or exits, the Tunnel server will set the Channel to CLOSING. A Channel in the CLOSING state is transient,
 * indicating that the Tunnel server is re-scheduling the Channel. In this state, the client must first change the Channel state in local memory to CLOSED, then report it through the heartbeat
 * so the Tunnel server can handle the unified state processing, Channel scheduling, and re-allocation.
 * 4. CLOSE: Closed state, when the TunnelClient encounters an error or normal shutdown, the Channel state will be set to CLOSE. A Channel in the CLOSE state is temporarily closed, similar to a WAIT
 * state Channel. When a TunnelClient re-calls the heartbeat interface, the CLOSE state Channel will be re-allocated to a new TunnelClient, and its state will become OPEN.
 * 5. TERMINATED: Terminated state, after data consumption on the Channel is completed, it will enter this state. Channels of the full-quantity type will eventually be fully consumed and transition into this state, while incremental Channels
 * will not enter this state (streaming data has no end).
 * <p>
 * ChannelConnect corresponds to Channel and has four states: WAIT, RUNNING, CLOSING, CLOSED. The relevant descriptions are as follows:
 * 1. WAIT: Waiting state, the initial state when a ChannelConnect is created.
 * 2. RUNNING: Running, this state indicates that the ChannelConnect is working well, continuously fetching data, processing data, and recording consumption checkpoints.
 * 3. CLOSING: Closing, corresponding to the CLOSING state of the Channel, during this state the ChannelConnect will interrupt data processing and transition to the CLOSED state.
 * 4. CLOSED: Closed, when a ChannelConnect is in this state, it can no longer consume data and needs to be removed from active connections. At the same time, its corresponding Channel needs to be updated to the CLOSE or TERMINATED state.
 */
public class TunnelStateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(TunnelStateMachine.class);

    private String tunnelId;
    private String clientId;

    private TunnelClientInterface client;
    /**
     * Create the dialer for ChannelConnect.
     */
    private IChannelDialer dialer;
    /**
     * Channel data processor with periodic Checkpoint functionality.
     */
    private IChannelProcessorFactory processorFactory;

    private volatile ConcurrentHashMap<String, IChannelConnect> channelConnects;
    private volatile ConcurrentHashMap<String, Channel> currentChannels;

    // In some extreme scenarios, a partition may be in the closing state. In this case, you can enable the detection of CLOSING partitions.
    private boolean enableClosingChannelDetect;
    private int channelClosingRoundThreshold = 3;
    private ConcurrentHashMap<String, Integer> channelClosingRounds;

    public TunnelStateMachine(String tunnelId, String clientId, IChannelDialer dialer,
                              IChannelProcessorFactory processorFactory, TunnelClientInterface client) {
        Preconditions.checkArgument(tunnelId != null && !tunnelId.isEmpty(),
                "The tunnel id should not be null or empty.");
        Preconditions.checkArgument(clientId != null && !clientId.isEmpty(),
                "The client id should not be null or empty.");
        Preconditions.checkNotNull(dialer, "Channel dialer cannot be null.");
        Preconditions.checkNotNull(processorFactory, "Channel process factory cannot be null.");
        Preconditions.checkNotNull(client, "Tunnel client cannot be null.");

        this.tunnelId = tunnelId;
        this.clientId = clientId;
        this.dialer = dialer;
        this.processorFactory = processorFactory;
        this.client = client;
        this.channelConnects = new ConcurrentHashMap<String, IChannelConnect>();
        this.currentChannels = new ConcurrentHashMap<String, Channel>();
        this.channelClosingRounds = new ConcurrentHashMap<String, Integer>();
    }

    /**
     * Update the active Channel information in the StateMachine and remove the ChannelConnect that is in the Closed state (with the same ChannelId).
     *
     * @param channel
     */
    public void updateStatus(Channel channel) {
        LOG.debug("Begin update channel status, channel: {}", channel);
        String channelId = channel.getChannelId();
        Channel currentChannel = currentChannels.get(channelId);
        if (currentChannel == null) {
            LOG.info("Redundant channel, channelId: {}, status: {}", channelId, channel.getStatus().name());
            return;
        }
        LOG.debug("CurrentChannel: {}, UpdateChannel: {}", currentChannel, channel);
        if (currentChannel.getVersion() >= channel.getVersion()) {
            LOG.info("Expired channel version, channelId: {}, current version: {}, old version: {}",
                    channelId, currentChannel.getVersion(), channel.getVersion());
            return;
        }
        currentChannels.put(channelId, channel);

        IChannelConnect channelConnect = channelConnects.get(channelId);
        if (channelConnect != null) {
            if (channelConnect.closed()) {
                channelConnects.remove(channelId);
            }
        }
    }

    public List<IChannelConnect> batchGetChannelConnects() {
        return new ArrayList<IChannelConnect>(channelConnects.values());
    }

    public List<Channel> batchGetChannels() {
        return new ArrayList<Channel>(currentChannels.values());
    }

    public void batchUpdateChannels(List<Channel> batchChannels) {
        LOG.info("Begin batch update channels");
        // First, perform a merge based on the version number in Channel to ensure that all Channels in CurrentChannels have larger version numbers.
        this.currentChannels = mergeChannels(batchChannels);
        // Update the status of ChannelConnect based on the Channel information.
        for (Map.Entry<String, Channel> entry : currentChannels.entrySet()) {
            String channelId = entry.getKey();
            IChannelConnect channelConnect = channelConnects.get(channelId);
            if (channelConnect == null) {
                try {
                    GetCheckpointResponse resp = client.getCheckpoint(
                            new GetCheckpointRequest(tunnelId, clientId, channelId));
                    LOG.info("Get checkpoint response, channelId: {}, checkpoint: {}, sequenceNumber: {}",
                            channelId, resp.getCheckpoint(), resp.getSequenceNumber());
                    // According to the user's input data processing Callback and the CheckpointInterval in TunnelWorkerConfig (the interval for recording data checkpoints to the server).
                    // Wrap a data processor with automatic Checkpoint recording functionality
                    IChannelProcessor channelProcessor = processorFactory.createProcessor(tunnelId, clientId, channelId,
                            new Checkpointer(client, tunnelId, clientId, channelId, resp.getSequenceNumber() + 1));
                    channelConnect = dialer.channelDial(tunnelId, clientId, channelId, resp.getCheckpoint(),
                            channelProcessor, this);
                    channelConnects.put(channelId, channelConnect);
                } catch (Exception e) {
                    LOG.warn("Failed to update channel, error detail: {}", e.toString());
                    channelConnect = new FailedChannelConnect(this);
                }
            }
            channelConnect.notifyStatus(entry.getValue());
        }

        // Clear invalid Channel information based on the currently active Channel connections.
        for (Map.Entry<String, IChannelConnect> entry : channelConnects.entrySet()) {
            String channelId = entry.getKey();
            Channel channel = currentChannels.get(channelId);
            if (channel == null) {
                LOG.info("Clear redundant channel connect, channelId: {}", channelId);
                IChannelConnect channelConnect = entry.getValue();
                if (!channelConnect.closed()) {
                    channelConnect.close();
                }
                channelConnects.remove(channelId);
            }
        }

        if (enableClosingChannelDetect) {
            handleHangedClosingChannels();
        }

    }

    private ConcurrentHashMap<String, Channel> mergeChannels(List<Channel> batchChannels) {
        ConcurrentHashMap<String, Channel> updatedChannels = new ConcurrentHashMap<String, Channel>(
                batchChannels.size());
        for (Channel channel : batchChannels) {
            String channelId = channel.getChannelId();
            Channel oldChannel = currentChannels.get(channelId);
            if (oldChannel != null) {
                if (channel.getVersion() >= oldChannel.getVersion()) {
                    updatedChannels.put(channelId, channel);
                } else {
                    updatedChannels.put(channelId, oldChannel);
                }
            } else {
                updatedChannels.put(channelId, channel);
            }
        }
        return updatedChannels;
    }

    public void close() {
        LOG.info("Begin close tunnel state machine, channelConnects: {}", channelConnects.size());
        for (IChannelConnect connect : channelConnects.values()) {
            connect.close();
        }
        LOG.info("Tunnel state machine is closed");
    }

    public void handleHangedClosingChannels() {
        LOG.info("Current closing channel detector, size: {}, detail: {}", channelClosingRounds.size(), channelClosingRounds);
        // Clear the statistics of invalid closing channels based on the currently active Channel connections.
        for (Map.Entry<String, Integer> entry : channelClosingRounds.entrySet()) {
            String channelId = entry.getKey();
            if (currentChannels.get(channelId) == null) {
                LOG.info("Clear redundant closing channel connect, channelId: {}", channelId);
                channelClosingRounds.remove(channelId);
            }
        }

        // When the channel has been in the closing state for multiple consecutive cycles, actively close the channel.
        for (Map.Entry<String, Channel> entry : currentChannels.entrySet()) {
            if (entry.getValue().getStatus() == ChannelStatus.CLOSING) {
                String channelId = entry.getKey();
                Integer rounds = channelClosingRounds.get(channelId);
                if (rounds == null) {
                    LOG.info("Add closing channel to detector, channelId: {}", channelId);
                    channelClosingRounds.put(channelId, 1);
                } else {
                    channelClosingRounds.put(channelId, rounds + 1);
                }
                if (channelClosingRounds.get(channelId) >= channelClosingRoundThreshold) {
                    LOG.info("Begin close closing channel via detector, channelId: {}, closing rounds: {}", channelId, rounds);
                    IChannelConnect channelConnect = channelConnects.get(channelId);
                    if (channelConnect != null && !channelConnect.closed()) {
                        LOG.info("Close closing channel via detector, channelId: {}", channelId);
                        channelConnect.close();
                        LOG.info("Finish closing channel via detector, channelId: {}", channelId);
                    }
                    channelClosingRounds.remove(channelId);
                }
            }
        }
    }

    public void setEnableClosingChannelDetect(boolean enableClosingChannelDetect) {
        this.enableClosingChannelDetect = enableClosingChannelDetect;
    }

    public void setChannelClosingRoundThreshold(int channelClosingRoundThreshold) {
        this.channelClosingRoundThreshold = channelClosingRoundThreshold;
    }

    public ConcurrentHashMap<String, Integer> getChannelClosingRounds() {
        return channelClosingRounds;
    }
}
