package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.DeleteRowRequest;
import com.aliyun.openservices.ots.model.DeleteRowResult;

public class DeleteRowCallable implements OTSCallable {

    private OTSAsyncDataOperation dataOperation;
    private OTSExecutionContext<DeleteRowRequest, DeleteRowResult> executionContext;

    public DeleteRowCallable(OTSAsyncDataOperation dataOperation, OTSExecutionContext<DeleteRowRequest, DeleteRowResult> executionContext) {
        this.dataOperation = dataOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.dataOperation.deleteRow(this.executionContext);
    }
}

