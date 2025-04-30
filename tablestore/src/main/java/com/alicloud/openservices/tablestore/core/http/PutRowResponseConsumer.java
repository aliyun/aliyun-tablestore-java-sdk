package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class PutRowResponseConsumer extends ResponseConsumer<PutRowResponse> {

    public PutRowResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, PutRowResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected PutRowResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.PutRowResponse putRowResponse =
            (OtsInternalApi.PutRowResponse) responseContent.getMessage();
        PutRowResponse result = ResponseFactory.createPutRowResponse(
            responseContent, putRowResponse);
        return result;
    }
        
}
