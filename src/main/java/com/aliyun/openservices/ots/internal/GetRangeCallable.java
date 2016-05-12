package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.GetRangeRequest;
import com.aliyun.openservices.ots.model.GetRangeResult;

public class GetRangeCallable implements OTSCallable {

    private OTSAsyncDataOperation dataOperation;
    private OTSExecutionContext<GetRangeRequest, GetRangeResult> executionContext;

    public GetRangeCallable(OTSAsyncDataOperation dataOperation, OTSExecutionContext<GetRangeRequest, GetRangeResult> executionContext) {
        this.dataOperation = dataOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.dataOperation.getRange(this.executionContext);
    }
}

