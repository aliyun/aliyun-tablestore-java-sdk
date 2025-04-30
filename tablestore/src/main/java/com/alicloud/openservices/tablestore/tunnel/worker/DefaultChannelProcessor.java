package com.alicloud.openservices.tablestore.tunnel.worker;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alicloud.openservices.tablestore.core.protocol.ResponseFactory.FINISH_TAG;

public class DefaultChannelProcessor implements IChannelProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultChannelProcessor.class);

    private final IChannelProcessor recordProcessor;
    private long latestCheckpoint = System.currentTimeMillis();
    private final long checkpointIntervalInMillis;
    private final ICheckpointer checkpointer;

    public DefaultChannelProcessor(IChannelProcessor recordProcessor, ICheckpointer checkpointer,
                                   long checkpointIntervalInMillis) {
        Preconditions.checkNotNull(recordProcessor, "Channel record processor cannot be null.");
        Preconditions.checkNotNull(checkpointer, "Checkpointer cannot be null.");

        this.recordProcessor = recordProcessor;
        this.checkpointer = checkpointer;
        this.checkpointIntervalInMillis = checkpointIntervalInMillis;
    }

    @Override
    public void process(ProcessRecordsInput input) {
        recordProcessor.process(input);

        // When the data has been processed to the FinishedTag, you need to record the checkpoint on the server side.
        if (input.getNextToken() == null || FINISH_TAG.equals(input.getNextToken())) {
            LOG.info("begin do checkpoint, token = {}", input.getNextToken());
            try {
                checkpointer.checkpoint(FINISH_TAG);
            } catch (Exception e) {
                LOG.error("checkpoint error, detail: {}", e.toString());
            }
        } else if (System.currentTimeMillis() - latestCheckpoint > checkpointIntervalInMillis) {
            LOG.info("begin do checkpoint, token = {}", input.getNextToken());
            try {
                checkpointer.checkpoint(input.getNextToken());
            } catch (Exception e) {
                LOG.error("checkpoint error, detail: {}", e.toString());
            }
            latestCheckpoint = System.currentTimeMillis();
        }
    }

    @Override
    public void shutdown() {
        recordProcessor.shutdown();
    }
}
