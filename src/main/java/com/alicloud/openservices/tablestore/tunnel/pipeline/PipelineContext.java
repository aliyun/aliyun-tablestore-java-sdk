package com.alicloud.openservices.tablestore.tunnel.pipeline;

/**
 * 对Pipeline的计算环境进行抽象, 进行错误处理等工作。
 */
public interface PipelineContext {
    void handleError(final StageException ex);
}
