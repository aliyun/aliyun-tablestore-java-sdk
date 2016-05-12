package com.aliyun.openservices.ots.internal;

public class BatchWriteRowCallable implements OTSCallable {

    private OTSAsyncDataOperation dataOperation;
    private BatchWriteRowExecutionContext executionContext;

    public BatchWriteRowCallable(OTSAsyncDataOperation dataOperation, BatchWriteRowExecutionContext executionContext) {
        this.dataOperation = dataOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.dataOperation.batchWriteRow(this.executionContext);
    }
}
