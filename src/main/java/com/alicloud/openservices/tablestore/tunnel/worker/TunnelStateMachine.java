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
 * TunnelStateMachine用于Channel和ChannelConnect状态的维护和更新(包括本机内存以及和Tunnel服务端的交互)。
 * Channel的状态有五类: WAIT, OPEN, CLOSING, CLOSE, TERMINATED。Channel状态的说明如下:
 * 1. WAIT: 等待状态，当Tunnel创建时，Tunnel服务端会预先创建一批待分配(WAIT)的Channel, 以供Tunnel客户端来连接。
 * 2. OPEN: 启动状态，当TunnelClient调用heartbeat接口时，Tunnel服务器会给TunnelClient分配Channel，并将这些Channel的状态置为OPEN.
 * 3. CLOSING: 关闭中状态，当有新的TunnelClient加入或者退出时，Tunnel服务端会把Channel置为Closing,Closing状态的Channel是一种暂态，
 * 表示Tunnel服务端正在进行Channel的重新调度，在此状态下，客户端必须先将本机内存中的Channel状态转为CLOSED, 再通过heartbeat上报给Tunnel服务端
 * 进行统一的状态处理，Channel调度和重新分配。
 * 4. CLOSE: 关闭状态，当TunnelClient出现错误或者正常的shutdown时，Channel的状态会被置为CLOSE，CLOSE状态的Channel只是暂时的关闭，和WAIT
 * 状态的Channel类似，当有TunnelClient重新调用heartbeat接口时，CLOSE状态的Channel会被重新分配到新的TunnelClient, 状态也会变成OPEN.
 * 5. TERMINATED: 结束状态，当Channel上的数据消费完成后，会进入此状态。全量类型的Channel最终都会被消费完成，转入此状态，而增量状态的Channel则
 * 不会进入此状态(流式数据无尽头)。
 * <p>
 * ChannelConnect和Channel对应，有WAIT, RUNNING, CLOSING, CLOSE四种状态, 相关说明如下:
 * 1. WAIT: 等待状态，ChannelConnect被创建出来的初始状态。
 * 2. RUNNING: 运行中，此状态代表ChannelConnect工作良好，正在不断的拉取数据，处理数据和记录消费位点。
 * 3. CLOSING: 关闭中，对应到Channel的CLOSING状态，处于此状态的ChannelConnect在数据处理过程中，会中断数据处理，并转为CLOSED状态。
 * 4. CLOSED: 已关闭，ChannelConnect处于此状态已经无法再进行数据消费了，需要从活跃连接中移除，同时需要将其对应的Channel更新为CLOSE或TERMINATED状态。
 */
public class TunnelStateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(TunnelStateMachine.class);

    private String tunnelId;
    private String clientId;

    private TunnelClientInterface client;
    /**
     * 创建ChannelConnect的拨号器。
     */
    private IChannelDialer dialer;
    /**
     * 带定期Checkpoint功能的Channel数据处理器。
     */
    private IChannelProcessorFactory processorFactory;

    private volatile ConcurrentHashMap<String, IChannelConnect> channelConnects;
    private volatile ConcurrentHashMap<String, Channel> currentChannels;

    // 某些极端场景下，可能分区会处于closing状态，此时可以开启CLOSING分区的检测
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
     * 更新StateMachine中的活跃Channel信息，同时移除(ChannelId相同的)处于Closed状态的ChannelConnect.
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
        // 根据Channel中的版本号先做Merge, 保证CurrentChannels里都是版本号较大的Channel。
        this.currentChannels = mergeChannels(batchChannels);
        // 根据Channel信息更新ChannelConnect的状态。
        for (Map.Entry<String, Channel> entry : currentChannels.entrySet()) {
            String channelId = entry.getKey();
            IChannelConnect channelConnect = channelConnects.get(channelId);
            if (channelConnect == null) {
                try {
                    GetCheckpointResponse resp = client.getCheckpoint(
                            new GetCheckpointRequest(tunnelId, clientId, channelId));
                    LOG.info("Get checkpoint response, channelId: {}, checkpoint: {}, sequenceNumber: {}",
                            channelId, resp.getCheckpoint(), resp.getSequenceNumber());
                    // 根据用户传入的处理数据的Callback和TunnelWorkerConfig中CheckpointInterval(向服务端记数据位点的间隔)
                    // 包装出一个带自动记Checkpoint功能的数据处理器
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

        // 根据当前活跃的Channel连接来清除无效的Channel信息。
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
        // 根据当前活跃的Channel连接来清除无效的closing channel统计
        for (Map.Entry<String, Integer> entry : channelClosingRounds.entrySet()) {
            String channelId = entry.getKey();
            if (currentChannels.get(channelId) == null) {
                LOG.info("Clear redundant closing channel connect, channelId: {}", channelId);
                channelClosingRounds.remove(channelId);
            }
        }

        // 当channel连续多个周期都为closing状态，则执行channel的主动关闭
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
