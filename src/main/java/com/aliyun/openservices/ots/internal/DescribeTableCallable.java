package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.DescribeTableRequest;
import com.aliyun.openservices.ots.model.DescribeTableResult;

public class DescribeTableCallable implements OTSCallable {

    private OTSAsyncTableOperation tableOperation;
    private OTSExecutionContext<DescribeTableRequest, DescribeTableResult> executionContext;

    public DescribeTableCallable(OTSAsyncTableOperation tableOperation, OTSExecutionContext<DescribeTableRequest, DescribeTableResult> executionContext) {
        this.tableOperation = tableOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.tableOperation.describeTable(this.executionContext);
    }

}

