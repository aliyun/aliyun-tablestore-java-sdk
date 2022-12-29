package com.alicloud.openservices.tablestore.tunnel.pipeline;

public abstract class AbstractStage<INPUT, OUTPUT> implements Stage<INPUT, OUTPUT> {
    /**
     * 当前阶段的下一个处理阶段。
     */
    private volatile Stage<OUTPUT, ?> nextStage;
    /**
     * 当前阶段的上下文。
     */
    private volatile PipelineContext context;

    @Override
    @SuppressWarnings("unchecked")
    public void setNextStage(Stage<?, ?> stage) {
        this.nextStage = (Stage<OUTPUT, ?>) stage;
    }

    @Override
    public void init(PipelineContext context) {
        this.context = context;
    }

    public abstract OUTPUT doProcess(INPUT input) throws StageException;

    @Override
    public void process(INPUT input) {
        try {
            OUTPUT output = doProcess(input);
            if (nextStage != null && output != null) {
                nextStage.process(output);
            }
        } catch (StageException se) {
            context.handleError(se);
        } catch (Exception e) {
            context.handleError(new StageException(this, input, "", e));
        }
    }

    @Override
    public void shutdown() {

    }

}
