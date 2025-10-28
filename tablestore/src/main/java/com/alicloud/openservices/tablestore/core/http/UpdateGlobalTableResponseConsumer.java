package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTable;
import com.alicloud.openservices.tablestore.model.UpdateGlobalTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class UpdateGlobalTableResponseConsumer extends ResponseConsumer<UpdateGlobalTableResponse> {

    public UpdateGlobalTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, UpdateGlobalTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected UpdateGlobalTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        GlobalTable.UpdateGlobalTableResponse protoResp =
            (GlobalTable.UpdateGlobalTableResponse) responseContent.getMessage();
        UpdateGlobalTableResponse result = ResponseFactory.createUpdateGlobalTableResponse(
            responseContent, protoResp);
        return result;
    }
}
