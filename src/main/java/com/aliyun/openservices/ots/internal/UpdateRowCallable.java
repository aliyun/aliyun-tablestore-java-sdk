package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.UpdateRowRequest;
import com.aliyun.openservices.ots.model.UpdateRowResult;

public class UpdateRowCallable implements OTSCallable {

    private OTSAsyncDataOperation dataOperation;
    private OTSExecutionContext<UpdateRowRequest, UpdateRowResult> executionContext;

    public UpdateRowCallable(OTSAsyncDataOperation dataOperation, OTSExecutionContext<UpdateRowRequest, UpdateRowResult> executionContext) {
        this.dataOperation = dataOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.dataOperation.updateRow(this.executionContext);
    }
}

