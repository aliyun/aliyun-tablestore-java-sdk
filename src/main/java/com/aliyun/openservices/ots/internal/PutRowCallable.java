package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.PutRowRequest;
import com.aliyun.openservices.ots.model.PutRowResult;

public class PutRowCallable implements OTSCallable {

    private OTSAsyncDataOperation dataOperation;
    private OTSExecutionContext<PutRowRequest, PutRowResult> executionContext;

    public PutRowCallable(OTSAsyncDataOperation dataOperation, OTSExecutionContext<PutRowRequest, PutRowResult> executionContext) {
        this.dataOperation = dataOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.dataOperation.putRow(this.executionContext);
    }
}

