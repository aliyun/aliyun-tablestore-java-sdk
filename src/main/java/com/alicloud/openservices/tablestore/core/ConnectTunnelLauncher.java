package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.ConnectTunnelResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.TunnelProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ConnectTunnelResponse;
import org.apache.http.concurrent.FutureCallback;

public class ConnectTunnelLauncher
    extends OperationLauncher<ConnectTunnelRequest, ConnectTunnelResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public ConnectTunnelLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        ConnectTunnelRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);

        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(ConnectTunnelRequest req, FutureCallback<ConnectTunnelResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        TunnelServiceApi.ConnectResponse defaultResponse =
            TunnelServiceApi.ConnectResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            TunnelProtocolBuilder.buildConnectTunnelRequest(req),
            tracer,
            new ConnectTunnelResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()), tracer, retry, lastResult),
            cb);
    }
}
