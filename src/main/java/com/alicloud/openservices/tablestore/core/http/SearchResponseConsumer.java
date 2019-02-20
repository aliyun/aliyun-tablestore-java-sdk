package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;

public class SearchResponseConsumer
        extends ResponseConsumer<SearchResponse> {

    public SearchResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, SearchResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected SearchResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        Search.SearchResponse internalResponse =
                (Search.SearchResponse) responseContent.getMessage();
        SearchResponse response = ResponseFactory.createSearchResponse(
                responseContent, internalResponse);
        return response;
    }
}
