package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.CreateTableRequest;
import com.aliyun.openservices.ots.model.CreateTableResult;

public class CreateTableCallable implements OTSCallable {

    private OTSAsyncTableOperation tableOperation;
    private OTSExecutionContext<CreateTableRequest, CreateTableResult> executionContext;

    public CreateTableCallable(OTSAsyncTableOperation tableOperation, OTSExecutionContext<CreateTableRequest, CreateTableResult> executionContext) {
        this.tableOperation = tableOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.tableOperation.createTable(this.executionContext);
    }

}

