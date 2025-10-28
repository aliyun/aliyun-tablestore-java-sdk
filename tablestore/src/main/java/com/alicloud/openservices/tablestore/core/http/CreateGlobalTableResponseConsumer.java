package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTable;
import com.alicloud.openservices.tablestore.model.CreateGlobalTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class CreateGlobalTableResponseConsumer extends ResponseConsumer<CreateGlobalTableResponse> {

    public CreateGlobalTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, CreateGlobalTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CreateGlobalTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        GlobalTable.CreateGlobalTableResponse protoResp =
            (GlobalTable.CreateGlobalTableResponse) responseContent.getMessage();
        CreateGlobalTableResponse result = ResponseFactory.createCreateGlobalTableResponse(
            responseContent, protoResp);
        return result;
    }
}
