package com.alicloud.openservices.tablestore.tunnel.pipeline;

import java.util.concurrent.ExecutorService;

public class ThreadPoolStageDecorator<INPUT, OUTPUT> implements Stage<INPUT, OUTPUT> {
    final Stage<INPUT, OUTPUT> stage;
    final ExecutorService executorService;

    public ThreadPoolStageDecorator(Stage<INPUT, OUTPUT> stage, ExecutorService executorService) {
        this.stage = stage;
        this.executorService = executorService;
    }

    @Override
    public void setNextStage(Stage<?, ?> nextPipe) {
        stage.setNextStage(nextPipe);
    }

    @Override
    public void init(PipelineContext context) {
        stage.init(context);
    }

    @Override
    public void process(final INPUT input) {
        Runnable task = new Runnable() {
            @Override
            public void run() {
                stage.process(input);
            }
        };

        executorService.submit(task);
    }

    public void shutdown() {
        executorService.shutdown();
    }
}
