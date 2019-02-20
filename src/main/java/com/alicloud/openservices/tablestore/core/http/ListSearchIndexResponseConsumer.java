package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class ListSearchIndexResponseConsumer
        extends ResponseConsumer<ListSearchIndexResponse> {

    public ListSearchIndexResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, ListSearchIndexResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ListSearchIndexResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Search.ListSearchIndexResponse internalResponse =
                (Search.ListSearchIndexResponse) responseContent.getMessage();
        ListSearchIndexResponse response = ResponseFactory.createListSearchIndexResponse(
                responseContent, internalResponse);
        return response;
    }
}
