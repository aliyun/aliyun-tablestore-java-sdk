package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.ComputeSplitsResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class ComputeSplitsResponseConsumer extends ResponseConsumer<ComputeSplitsResponse> {

    public ComputeSplitsResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, ComputeSplitsResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ComputeSplitsResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.ComputeSplitsResponse internalResponse = (OtsInternalApi.ComputeSplitsResponse) responseContent.getMessage();
        return ResponseFactory.createComputeSplitsResponse(responseContent, internalResponse);
    }
}
