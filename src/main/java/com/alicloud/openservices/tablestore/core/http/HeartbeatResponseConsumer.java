package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.tunnel.internal.HeartbeatResponse;

public class HeartbeatResponseConsumer
    extends ResponseConsumer<HeartbeatResponse> {
    public HeartbeatResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, HeartbeatResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected HeartbeatResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.HeartbeatResponse tunnelResponse =
            (TunnelServiceApi.HeartbeatResponse)responseContent.getMessage();
        return ResponseFactory.createHeartbeatResponse(responseContent, tunnelResponse);
    }
}
