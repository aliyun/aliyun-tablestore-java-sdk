package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class GetRangeResponseConsumer extends ResponseConsumer<GetRangeResponse> {

    public GetRangeResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, GetRangeResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected GetRangeResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.GetRangeResponse GetRangeResponse =
            (OtsInternalApi.GetRangeResponse) responseContent.getMessage();
        GetRangeResponse result = ResponseFactory.createGetRangeResponse(
            responseContent, GetRangeResponse);
        return result;
    }
        
}
