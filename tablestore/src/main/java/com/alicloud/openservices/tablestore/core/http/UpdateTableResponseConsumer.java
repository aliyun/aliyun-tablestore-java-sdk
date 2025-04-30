package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.UpdateTableResponse;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class UpdateTableResponseConsumer
  extends ResponseConsumer<UpdateTableResponse> {

    public UpdateTableResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, UpdateTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected UpdateTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.UpdateTableResponse internalResponse =
            (OtsInternalApi.UpdateTableResponse) responseContent.getMessage();
        UpdateTableResponse response = ResponseFactory.createUpdateTableResponse(
            responseContent, internalResponse);
        return response;
    }
}
