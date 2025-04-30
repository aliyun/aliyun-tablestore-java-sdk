package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.tunnel.internal.GetCheckpointResponse;

public class GetCheckpointResponseConsumer
    extends ResponseConsumer<GetCheckpointResponse> {
    public GetCheckpointResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, GetCheckpointResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected GetCheckpointResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.GetCheckpointResponse tunnelResponse =
            (TunnelServiceApi.GetCheckpointResponse)responseContent.getMessage();
        return ResponseFactory.createGetCheckpointResponse(responseContent, tunnelResponse);
    }
}
