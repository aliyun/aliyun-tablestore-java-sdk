package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.search.UpdateSearchIndexResponse;

public class UpdateSearchIndexResponseConsumer
    extends ResponseConsumer<UpdateSearchIndexResponse>  {

    public UpdateSearchIndexResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, UpdateSearchIndexResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected UpdateSearchIndexResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Search.UpdateSearchIndexResponse internalResponse =
                (Search.UpdateSearchIndexResponse) responseContent.getMessage();
        UpdateSearchIndexResponse response = ResponseFactory.createUpdateSearchIndexResponse(
                responseContent, internalResponse);
        return response;
    }
}
