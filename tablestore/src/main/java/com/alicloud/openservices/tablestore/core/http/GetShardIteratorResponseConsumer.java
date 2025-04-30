package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.GetShardIteratorResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class GetShardIteratorResponseConsumer
        extends ResponseConsumer<GetShardIteratorResponse> {

    public GetShardIteratorResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, GetShardIteratorResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected GetShardIteratorResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.GetShardIteratorResponse internalResponse =
                (OtsInternalApi.GetShardIteratorResponse) responseContent.getMessage();
        GetShardIteratorResponse response = ResponseFactory.createGetShardIteratorResponse(
                responseContent, internalResponse);
        return response;
    }
}
