package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DescribeTunnelResponseConsumer
    extends ResponseConsumer<DescribeTunnelResponse> {
    public DescribeTunnelResponseConsumer(
        ResultParser resultParser, TraceLogger traceLogger,
        RetryStrategy retry, DescribeTunnelResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DescribeTunnelResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        TunnelServiceApi.DescribeTunnelResponse tunnelResponse =
            (TunnelServiceApi.DescribeTunnelResponse) responseContent.getMessage();
        DescribeTunnelResponse response = ResponseFactory.createDescribeTunnelResponse(responseContent, tunnelResponse);
        return response;
    }
}
