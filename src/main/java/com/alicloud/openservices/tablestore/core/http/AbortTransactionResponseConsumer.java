package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.AbortTransactionResponse;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class AbortTransactionResponseConsumer
        extends ResponseConsumer<AbortTransactionResponse> {

    public AbortTransactionResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, AbortTransactionResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected AbortTransactionResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.AbortTransactionResponse internalResponse =
                (OtsInternalApi.AbortTransactionResponse) responseContent.getMessage();
        AbortTransactionResponse response = ResponseFactory.createAbortTransactionResponse(
                responseContent, internalResponse);
        return response;
    }
}
