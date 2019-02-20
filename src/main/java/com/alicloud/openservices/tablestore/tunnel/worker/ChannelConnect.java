package com.alicloud.openservices.tablestore.tunnel.worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.alicloud.openservices.tablestore.TunnelClientInterface;
import com.alicloud.openservices.tablestore.model.tunnel.ChannelStatus;
import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.tunnel.pipeline.ProcessDataPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ChannelConnect维护数据消费的状态和所需的相关资源。
 * ChannelConnect的状态和其对应的Channel息息相关，对应的细节可参见: {@link TunnelStateMachine}的说明。
 */
public class ChannelConnect implements IChannelConnect {
    private static final Logger LOG = LoggerFactory.getLogger(ChannelConnect.class);

    private String tunnelId;
    private String clientId;
    private String channelId;
    private String token;

    private TunnelClientInterface client;
    private IChannelProcessor processor;
    private Channel currentChannel;
    private TunnelStateMachine stateMachine;
    private AtomicReference<ChannelConnectStatus> status = new AtomicReference<ChannelConnectStatus>(ChannelConnectStatus.WAIT);
    /**
     * Channel的数据是否已经拉完了，拉到finished token值了(全量类型的Channel最终会拉完)
     */
    private AtomicBoolean finished = new AtomicBoolean(false);
    /**
     * 是否为增量类型的Channel, 增量类型的Channel单次数据拉取不足时需要加退避。
     */
    private boolean streamChannel;
    private ICheckpointer checkpointer;
    private ProcessDataPipeline processPipeline;
    /**
     * 用于Channel提交Pipeline任务(读数据,处理数据等)
     */
    private ExecutorService channelExecutorService;
    private ThreadPoolExecutor readRecordsExecutor;
    private ThreadPoolExecutor processRecordsExecutor;

    @Override
    public synchronized void notifyStatus(Channel channel) {
        LOG.debug("Begin notify status, channel: {}", channel);
        if (currentChannel != null && currentChannel.getVersion() > channel.getVersion()) {
            return;
        }
        currentChannel = new Channel(channel);
        switch (currentChannel.getStatus()) {
            case CLOSE:
                LOG.info("Closed channel status {}", this);
                close(false);
                break;
            case CLOSING:
                //draw closing action and check closed/finish status
                if (status.get() == ChannelConnectStatus.WAIT) {
                    status.set(ChannelConnectStatus.CLOSED);
                } else {
                    status.compareAndSet(ChannelConnectStatus.RUNNING, ChannelConnectStatus.CLOSING);
                }
                checkAndUpdateChannelStatus();
                break;
            case OPEN:
                if (status.compareAndSet(ChannelConnectStatus.WAIT, ChannelConnectStatus.RUNNING)) {
                    // 提交pipeline任务，pipeline会循环的进行数据处理
                    LOG.info("Submit pipeline task, channel connect :{}", this);
                    if (channelExecutorService != null) {
                        channelExecutorService.submit(processPipeline);
                    }
                } else {
                    checkAndUpdateChannelStatus();
                }
                break;
            case TERMINATED:
                LOG.info("Terminated channel status {}", this);
                close(true);
                break;
            default:
                LOG.warn("Unexpected channel status {}", this);
        }
    }

    public void checkAndUpdateChannelStatus() {
        LOG.debug("Check update status, ChannelConnectStatus: {}", status.get().name());
        if (status.get() == ChannelConnectStatus.CLOSED) {
            currentChannel.setVersion(currentChannel.getVersion() + 1);
            if (finished.get()) {
                currentChannel.setStatus(ChannelStatus.TERMINATED);
            } else {
                currentChannel.setStatus(ChannelStatus.CLOSE);
            }

            LOG.info("Update channel status, current channel: {}", currentChannel);
            stateMachine.updateStatus(currentChannel);
        }
    }

    @Override
    public boolean closed() {
        return status.get() == ChannelConnectStatus.CLOSED;
    }

    @Override
    public void close() {
        close(false);
    }

    public void close(boolean finish) {
        if (status.get() != ChannelConnectStatus.CLOSED) {
            LOG.info("Shutdown Channel connect.");
            if (processor != null) {
                processor.shutdown();
            }
            if (finish) {
                finished.set(true);
            }
            status.set(ChannelConnectStatus.CLOSED);
            LOG.info("After close, ChannelConnectStatus: {}", status.get());
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append("TunnelId: ").append(tunnelId).append(", ClientId: ").append(clientId)
            .append(", ChannelId: ").append(channelId).append(", CurrentChannel: ")
            .append(currentChannel).append("]");
        return sb.toString();
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

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public TunnelClientInterface getClient() {
        return client;
    }

    public void setClient(TunnelClientInterface client) {
        this.client = client;
    }

    public IChannelProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(IChannelProcessor processor) {
        this.processor = processor;
    }

    public Channel getCurrentChannel() {
        return currentChannel;
    }

    public void setCurrentChannel(Channel currentChannel) {
        this.currentChannel = currentChannel;
    }

    public TunnelStateMachine getStateMachine() {
        return stateMachine;
    }

    public void setStateMachine(TunnelStateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    public ChannelConnectStatus getStatus() {
        return status.get();
    }

    public void setStatus(ChannelConnectStatus status) {
        this.status.set(status);
    }

    public AtomicBoolean getFinished() {
        return finished;
    }

    public void setFinished(AtomicBoolean finished) {
        this.finished = finished;
    }

    public boolean isStreamChannel() {
        return streamChannel;
    }

    public void setStreamChannel(boolean streamChannel) {
        this.streamChannel = streamChannel;
    }

    public ICheckpointer getCheckpointer() {
        return checkpointer;
    }

    public void setCheckpointer(ICheckpointer checkpointer) {
        this.checkpointer = checkpointer;
    }

    public ProcessDataPipeline getProcessPipeline() {
        return processPipeline;
    }

    public void setProcessPipeline(
        ProcessDataPipeline processPipeline) {
        this.processPipeline = processPipeline;
    }

    public ExecutorService getChannelExecutorService() {
        return channelExecutorService;
    }

    public void setChannelExecutorService(ExecutorService channelExecutorService) {
        this.channelExecutorService = channelExecutorService;
    }
}



