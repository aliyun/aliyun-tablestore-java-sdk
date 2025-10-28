package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTable;
import com.alicloud.openservices.tablestore.model.BindGlobalTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class BindGlobalTableResponseConsumer extends ResponseConsumer<BindGlobalTableResponse> {

    public BindGlobalTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, BindGlobalTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected BindGlobalTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        GlobalTable.BindGlobalTableResponse protoResp =
            (GlobalTable.BindGlobalTableResponse) responseContent.getMessage();
        BindGlobalTableResponse result = ResponseFactory.createBindGlobalTableResponse(
            responseContent, protoResp);
        return result;
    }
}
