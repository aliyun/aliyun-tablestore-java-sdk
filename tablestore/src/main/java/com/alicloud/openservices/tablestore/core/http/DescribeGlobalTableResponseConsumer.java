package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTable;
import com.alicloud.openservices.tablestore.model.DescribeGlobalTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DescribeGlobalTableResponseConsumer extends ResponseConsumer<DescribeGlobalTableResponse> {

    public DescribeGlobalTableResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, DescribeGlobalTableResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DescribeGlobalTableResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        GlobalTable.DescribeGlobalTableResponse protoResp =
            (GlobalTable.DescribeGlobalTableResponse) responseContent.getMessage();
        DescribeGlobalTableResponse result = ResponseFactory.createDescribeGlobalTableResponse(
            responseContent, protoResp);
        return result;
    }
}
