package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.DeleteDefinedColumnResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class DeleteDefinedColumnResponseConsumer
  extends ResponseConsumer<DeleteDefinedColumnResponse> {

    public DeleteDefinedColumnResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, DeleteDefinedColumnResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }
    @Override
    protected DeleteDefinedColumnResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.DeleteDefinedColumnResponse internalResponse =
            (OtsInternalApi.DeleteDefinedColumnResponse) responseContent.getMessage();
        DeleteDefinedColumnResponse response = ResponseFactory.createDeleteDefinedColumnResponse(
            responseContent, internalResponse);
        return response;
    }
}
