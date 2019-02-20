package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class DescribeSearchIndexResponseConsumer
        extends ResponseConsumer<DescribeSearchIndexResponse> {

    public DescribeSearchIndexResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, DescribeSearchIndexResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DescribeSearchIndexResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Search.DescribeSearchIndexResponse internalResponse =
                (Search.DescribeSearchIndexResponse) responseContent.getMessage();
        DescribeSearchIndexResponse response = ResponseFactory.createDescribeSearchIndexResponse(
                responseContent, internalResponse);
        return response;
    }
}
