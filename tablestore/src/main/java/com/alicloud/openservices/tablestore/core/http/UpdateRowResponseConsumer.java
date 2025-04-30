package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.UpdateRowResponse;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class UpdateRowResponseConsumer extends ResponseConsumer<UpdateRowResponse> {

    public UpdateRowResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, UpdateRowResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected UpdateRowResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.UpdateRowResponse UpdateRowResponse =
            (OtsInternalApi.UpdateRowResponse) responseContent.getMessage();
        UpdateRowResponse result = ResponseFactory.createUpdateRowResponse(
            responseContent, UpdateRowResponse);
        return result;
    }
        
}
