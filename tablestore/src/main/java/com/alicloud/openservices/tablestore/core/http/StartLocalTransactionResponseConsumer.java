package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.AbortTransactionResponse;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionResponse;

public class StartLocalTransactionResponseConsumer
        extends ResponseConsumer<StartLocalTransactionResponse> {

    public StartLocalTransactionResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, StartLocalTransactionResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected StartLocalTransactionResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.StartLocalTransactionResponse internalResponse =
                (OtsInternalApi.StartLocalTransactionResponse) responseContent.getMessage();
        StartLocalTransactionResponse response = ResponseFactory.createStartLocalTransactionResponse(
                responseContent, internalResponse);
        return response;
    }
}
