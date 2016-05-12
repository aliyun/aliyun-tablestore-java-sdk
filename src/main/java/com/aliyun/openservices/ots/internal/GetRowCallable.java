package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.GetRowRequest;
import com.aliyun.openservices.ots.model.GetRowResult;

public class GetRowCallable implements OTSCallable {

    private OTSAsyncDataOperation dataOperation;
    private OTSExecutionContext<GetRowRequest, GetRowResult> executionContext;

    public GetRowCallable(OTSAsyncDataOperation dataOperation, OTSExecutionContext<GetRowRequest, GetRowResult> executionContext) {
        this.dataOperation = dataOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.dataOperation.getRow(this.executionContext);
    }
}

