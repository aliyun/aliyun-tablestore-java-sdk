package com.alicloud.openservices.tablestore.tunnel.pipeline;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pipeline<INPUT, OUTPUT> extends AbstractStage<INPUT, OUTPUT> {
    private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);

    private final List<Stage<?, ?>> stages = new LinkedList<Stage<?, ?>>();
    /**
     * helperExecutor is used for initialization, error handling during runtime, etc.
     */
    private final ExecutorService helperExecutor;

    public Pipeline(final ExecutorService helperExecutor) {
        super();
        this.helperExecutor = helperExecutor;
    }

    /**
     * Initialize the correlation (before/after relationship) of each Stage.
     *
     * @param context
     */
    @Override
    public void init(PipelineContext context) {
        Stage<?, ?> preStage = this;
        for (Stage<?, ?> stage : stages) {
            preStage.setNextStage(stage);
            preStage = stage;
        }
        helperExecutor.submit(new PipelineInitTask(context, stages));
    }

    static class PipelineInitTask implements Runnable {
        final List<Stage<?, ?>> stages;
        final PipelineContext context;

        public PipelineInitTask(PipelineContext context, List<Stage<?, ?>> stages) {
            this.context = context;
            this.stages = stages;
        }

        @Override
        public void run() {
            try {
                for (Stage<?, ?> stage : stages) {
                    stage.init(context);
                }
            } catch (Exception e) {
                LOG.error("Pipeline Init Error", e);
            }
        }
    }


    public <INPUT, OUTPUT> void addExecutorForStage(Stage<INPUT, OUTPUT> stage, ExecutorService executorService) {
        stages.add(new ThreadPoolStageDecorator<INPUT, OUTPUT>(stage, executorService));
    }


    @Override
    @SuppressWarnings("unchecked")
    public void process(INPUT input) {
        if (!stages.isEmpty()) {
            Stage<INPUT, ?> firstStage = (Stage<INPUT, ?>)stages.get(0);
            firstStage.process(input);
        }
    }

    @Override
    public OUTPUT doProcess(INPUT input) throws StageException {
        return null;
    }

    @Override
    public void shutdown() {
        shutdown(false);
    }

    /**
     * Close the current Pipeline.
     * @param isHalt: true means the thread pool resources need to be closed, false means not to close.
     */
    public void shutdown(boolean isHalt) {
        for (Stage<?, ?> stage : stages) {
            stage.shutdown();
        }
        if (isHalt) {
            LOG.info("shutdown pipeline helper executor.");
            helperExecutor.shutdownNow();
        }
    }
}
