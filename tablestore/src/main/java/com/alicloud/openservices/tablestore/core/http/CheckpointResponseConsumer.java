package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.tunnel.internal.CheckpointResponse;

public class CheckpointResponseConsumer
    extends ResponseConsumer<CheckpointResponse> {
    public CheckpointResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, CheckpointResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CheckpointResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.CheckpointResponse tunnelResponse =
            (TunnelServiceApi.CheckpointResponse)responseContent.getMessage();
        CheckpointResponse response = ResponseFactory.createCheckpointResponse(responseContent, tunnelResponse);
        return response;
    }
}
