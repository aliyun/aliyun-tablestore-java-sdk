package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class GetRowResponseConsumer extends ResponseConsumer<GetRowResponse> {

    public GetRowResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, GetRowResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected GetRowResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.GetRowResponse GetRowResponse =
            (OtsInternalApi.GetRowResponse) responseContent.getMessage();
        GetRowResponse result = ResponseFactory.createGetRowResponse(
            responseContent, GetRowResponse);
        return result;
    }
        
}
