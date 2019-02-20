package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.DescribeStreamResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DescribeStreamResponseConsumer
        extends ResponseConsumer<DescribeStreamResponse> {

    public DescribeStreamResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, DescribeStreamResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DescribeStreamResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.DescribeStreamResponse internalResponse =
                (OtsInternalApi.DescribeStreamResponse) responseContent.getMessage();
        DescribeStreamResponse response = ResponseFactory.createDescribeStreamResponse(
                responseContent, internalResponse);
        return response;
    }
}
