package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class CreateTableResponseConsumer
  extends ResponseConsumer<CreateTableResponse> {

    public CreateTableResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, CreateTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CreateTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.CreateTableResponse internalResponse =
            (OtsInternalApi.CreateTableResponse) responseContent.getMessage();
        CreateTableResponse response = ResponseFactory.createCreateTableResponse(
            responseContent, internalResponse);
        return response;
    }
}
