package com.alicloud.openservices.tablestore.tunnel.pipeline;

/**
 * Abstract the computing environment of the Pipeline, and perform error handling and other tasks.
 */
public interface PipelineContext {
    void handleError(final StageException ex);
}
