package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class DescribeTableResponseConsumer
  extends ResponseConsumer<DescribeTableResponse> {

    public DescribeTableResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger, 
        RetryStrategy retry, DescribeTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DescribeTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.DescribeTableResponse internalResponse =
            (OtsInternalApi.DescribeTableResponse) responseContent.getMessage();
        DescribeTableResponse response = ResponseFactory.createDescribeTableResponse(
            responseContent, internalResponse);
        return response;
    }
}
