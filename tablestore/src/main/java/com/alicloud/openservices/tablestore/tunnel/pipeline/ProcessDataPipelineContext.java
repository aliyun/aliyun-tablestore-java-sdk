package com.alicloud.openservices.tablestore.tunnel.pipeline;

import com.alicloud.openservices.tablestore.model.tunnel.internal.Channel;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelConnect;
import com.alicloud.openservices.tablestore.tunnel.worker.ChannelConnectStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

/**
 * The abstract of ProcessDataPipeline computing environment, mainly for error handling and other operations.
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
        // A ChannelConnect in CLOSED state means it has been closed. For ChannelConnect in other states, if an exception occurs, the ChannelConnect needs to be closed.
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
