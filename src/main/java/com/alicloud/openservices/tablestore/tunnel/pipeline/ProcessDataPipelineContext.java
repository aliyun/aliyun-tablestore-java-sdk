package com.alicloud.openservices.tablestore.tunnel.pipeline;

import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelConnect;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelConnectStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

/**
 * ProcessDataPipeline计算环境的抽象，主要进行错误处理等操作。
 */
public class ProcessDataPipelineContext implements PipelineContext {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessDataPipelineContext.class);
    private ChannelConnect connect;
    private Semaphore semaphore;

    public ProcessDataPipelineContext(ChannelConnect connect) {
        this.connect = connect;
    }

    public ProcessDataPipelineContext(ChannelConnect connect, Semaphore semaphore) {
        this.connect = connect;
        this.semaphore = semaphore;
    }

    @Override
    public void handleError(StageException ex) {
        // CLOSED状态的ChannelConnect代表已经被关闭了, 其它状态的ChannelConnect发生异常需要将ChannelConnect关闭。
        if (connect.getStatus() != ChannelConnectStatus.CLOSED) {
            LOG.warn("Channel connect will be closed, channelId: {}, error detail: [{},{}, {}]",
                connect.getChannelId(), ex.getCause(), ex.getMessage(), ex.getStackTrace());
            connect.close(false);
        }
        if (semaphore != null) {
            semaphore.release();
            LOG.info("Tunnel {}, Channel {} release semaphore after channel close, remaining: {}",
                    connect.getTunnelId(), connect.getChannelId(), semaphore.availablePermits());
        }
    }
}
