package com.aliyun.openservices.ots.internal;

public class BatchGetRowCallable implements OTSCallable {

    private OTSAsyncDataOperation dataOperation;
    private BatchGetRowExecutionContext executionContext;

    public BatchGetRowCallable(OTSAsyncDataOperation dataOperation, BatchGetRowExecutionContext executionContext) {
        this.dataOperation = dataOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.dataOperation.batchGetRow(this.executionContext);
    }
}
