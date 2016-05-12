package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.UpdateTableRequest;
import com.aliyun.openservices.ots.model.UpdateTableResult;

public class UpdateTableCallable implements OTSCallable {

    private OTSAsyncTableOperation tableOperation;
    private OTSExecutionContext<UpdateTableRequest, UpdateTableResult> executionContext;

    public UpdateTableCallable(OTSAsyncTableOperation tableOperation, OTSExecutionContext<UpdateTableRequest, UpdateTableResult> executionContext) {
        this.tableOperation = tableOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.tableOperation.updateTable(this.executionContext);
    }

}

