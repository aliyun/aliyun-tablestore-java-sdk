package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DeleteTunnelResponseConsumer
    extends ResponseConsumer<DeleteTunnelResponse> {
    public DeleteTunnelResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, DeleteTunnelResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteTunnelResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.DeleteTunnelResponse tunnelResponse =
            (TunnelServiceApi.DeleteTunnelResponse)responseContent.getMessage();
        DeleteTunnelResponse response = ResponseFactory.createDeleteTunnelResponse(responseContent, tunnelResponse);
        return response;
    }
}
