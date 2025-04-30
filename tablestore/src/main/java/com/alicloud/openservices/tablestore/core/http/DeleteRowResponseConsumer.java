package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.DeleteRowResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class DeleteRowResponseConsumer extends ResponseConsumer<DeleteRowResponse> {

    public DeleteRowResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, DeleteRowResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteRowResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.DeleteRowResponse DeleteRowResponse =
            (OtsInternalApi.DeleteRowResponse) responseContent.getMessage();
        DeleteRowResponse result = ResponseFactory.createDeleteRowResponse(
            responseContent, DeleteRowResponse);
        return result;
    }
        
}
