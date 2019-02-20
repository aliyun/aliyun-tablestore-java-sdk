package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.model.search.DeleteSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class DeleteSearchIndexResponseConsumer
        extends ResponseConsumer<DeleteSearchIndexResponse> {

    public DeleteSearchIndexResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, DeleteSearchIndexResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteSearchIndexResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Search.DeleteSearchIndexResponse internalResponse =
                (Search.DeleteSearchIndexResponse) responseContent.getMessage();
        DeleteSearchIndexResponse response = ResponseFactory.createDeleteSearchIndexResponse(
                responseContent, internalResponse);
        return response;
    }
}
