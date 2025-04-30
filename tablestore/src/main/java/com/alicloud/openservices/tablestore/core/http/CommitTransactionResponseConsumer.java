package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.AbortTransactionResponse;
import com.alicloud.openservices.tablestore.model.CommitTransactionResponse;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class CommitTransactionResponseConsumer
        extends ResponseConsumer<CommitTransactionResponse> {

    public CommitTransactionResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, CommitTransactionResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CommitTransactionResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.CommitTransactionResponse internalResponse =
                (OtsInternalApi.CommitTransactionResponse) responseContent.getMessage();
        CommitTransactionResponse response = ResponseFactory.createCommitTransactionResponse(
                responseContent, internalResponse);
        return response;
    }
}
