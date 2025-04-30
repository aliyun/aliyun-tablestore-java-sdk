package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.search.ParallelScanResponse;

public class ParallelScanResponseConsumer extends ResponseConsumer<ParallelScanResponse> {

    public ParallelScanResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, ParallelScanResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ParallelScanResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Search.ParallelScanResponse internalResponse = (Search.ParallelScanResponse) responseContent.getMessage();
        return ResponseFactory.createParallelScanResponse(responseContent, internalResponse);
    }
}
