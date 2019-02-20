package com.alicloud.openservices.tablestore.tunnel.pipeline;

public interface Stage<INPUT, OUTPUT> {
    void init(PipelineContext context);

    void setNextStage(Stage<?, ?> stage);

    void process(INPUT input);

    void shutdown();
}
