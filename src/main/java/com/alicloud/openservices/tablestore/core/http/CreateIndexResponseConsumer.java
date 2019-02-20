package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.CreateIndexResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class CreateIndexResponseConsumer
  extends ResponseConsumer<CreateIndexResponse> {

    public CreateIndexResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, CreateIndexResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CreateIndexResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.CreateIndexResponse internalResponse =
            (OtsInternalApi.CreateIndexResponse) responseContent.getMessage();
        CreateIndexResponse response = ResponseFactory.createCreteIndexResponse(
            responseContent, internalResponse);
        return response;
    }
}
