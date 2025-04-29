package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.DeleteTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class DeleteTableResponseConsumer
  extends ResponseConsumer<DeleteTableResponse> {

    public DeleteTableResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, DeleteTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.DeleteTableResponse internalResponse =
            (OtsInternalApi.DeleteTableResponse) responseContent.getMessage();
        DeleteTableResponse response = ResponseFactory.createDeleteTableResponse(
            responseContent, internalResponse);
        return response;
    }
}
