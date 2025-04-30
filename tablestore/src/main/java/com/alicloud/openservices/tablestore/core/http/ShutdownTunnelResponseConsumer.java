package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ShutdownTunnelResponse;

public class ShutdownTunnelResponseConsumer
    extends ResponseConsumer<ShutdownTunnelResponse> {
    public ShutdownTunnelResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, ShutdownTunnelResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ShutdownTunnelResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.ShutdownResponse tunnelResponse =
            (TunnelServiceApi.ShutdownResponse)responseContent.getMessage();
        return ResponseFactory.createShutdownTunnelResponse(responseContent, tunnelResponse);
    }
}
