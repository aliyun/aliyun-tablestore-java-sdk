package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.model.ListStreamResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;

public class ListStreamResponseConsumer
        extends ResponseConsumer<ListStreamResponse> {

    public ListStreamResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, ListStreamResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ListStreamResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.ListStreamResponse internalResponse =
                (OtsInternalApi.ListStreamResponse) responseContent.getMessage();
        ListStreamResponse response = ResponseFactory.createListStreamResponse(
                responseContent, internalResponse);
        return response;
    }
}
