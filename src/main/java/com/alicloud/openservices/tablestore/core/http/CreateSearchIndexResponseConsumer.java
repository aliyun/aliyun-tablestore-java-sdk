package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.model.search.CreateSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class CreateSearchIndexResponseConsumer
        extends ResponseConsumer<CreateSearchIndexResponse> {

    public CreateSearchIndexResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, CreateSearchIndexResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CreateSearchIndexResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Search.CreateSearchIndexResponse internalResponse =
                (Search.CreateSearchIndexResponse) responseContent.getMessage();
        CreateSearchIndexResponse response = ResponseFactory.createCreateSearchIndexResponse(
                responseContent, internalResponse);
        return response;
    }
}
