package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.DeleteIndexResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class DeleteIndexResponseConsumer
  extends ResponseConsumer<DeleteIndexResponse> {

    public DeleteIndexResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, DeleteIndexResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteIndexResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.DropIndexResponse internalResponse =
            (OtsInternalApi.DropIndexResponse) responseContent.getMessage();
        DeleteIndexResponse response = ResponseFactory.createDeleteIndexResponse(
            responseContent, internalResponse);
        return response;
    }
}
