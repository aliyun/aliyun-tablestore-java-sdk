package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.model.ListTableRequest;
import com.aliyun.openservices.ots.model.ListTableResult;

public class ListTableCallable implements OTSCallable {

    private OTSAsyncTableOperation tableOperation;
    private OTSExecutionContext<ListTableRequest, ListTableResult> executionContext;

    public ListTableCallable(OTSAsyncTableOperation tableOperation, OTSExecutionContext<ListTableRequest, ListTableResult> executionContext) {
        this.tableOperation = tableOperation;
        this.executionContext = executionContext;
    }

    @Override
    public void call() {
        this.tableOperation.listTable(this.executionContext);
    }

}

