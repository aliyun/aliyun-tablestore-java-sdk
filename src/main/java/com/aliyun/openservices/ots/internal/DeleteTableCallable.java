package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.DeleteTableRequest;
import com.aliyun.openservices.ots.model.DeleteTableResult;

public class DeleteTableCallable implements OTSCallable {

    private OTSAsyncTableOperation tableOperation;
    private OTSExecutionContext<DeleteTableRequest, DeleteTableResult> executionContext;

    public DeleteTableCallable(OTSAsyncTableOperation tableOperation, OTSExecutionContext<DeleteTableRequest, DeleteTableResult> executionContext) {
        this.tableOperation = tableOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.tableOperation.deleteTable(this.executionContext);
    }

}

