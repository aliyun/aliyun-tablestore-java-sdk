package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTable;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.UnbindGlobalTableResponse;

public class UnbindGlobalTableResponseConsumer extends ResponseConsumer<UnbindGlobalTableResponse> {

    public UnbindGlobalTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, UnbindGlobalTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected UnbindGlobalTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        GlobalTable.UnbindGlobalTableResponse protoResp =
            (GlobalTable.UnbindGlobalTableResponse) responseContent.getMessage();
        UnbindGlobalTableResponse result = ResponseFactory.createUnbindGlobalTableResponse(
            responseContent, protoResp);
        return result;
    }
}
