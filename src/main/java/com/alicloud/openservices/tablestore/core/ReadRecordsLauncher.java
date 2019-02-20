package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.ReadRecordsResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.TunnelProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.TunnelServiceApi;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsRequest;
import com.alicloud.openservices.tablestore.model.tunnel.internal.ReadRecordsResponse;
import org.apache.http.concurrent.FutureCallback;

public class ReadRecordsLauncher
    extends OperationLauncher<ReadRecordsRequest, ReadRecordsResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public ReadRecordsLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        ReadRecordsRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);

        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(ReadRecordsRequest req, FutureCallback<ReadRecordsResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        TunnelServiceApi.ReadRecordsResponse defaultResponse =
            TunnelServiceApi.ReadRecordsResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            TunnelProtocolBuilder.buildReadRecordsRequest(req),
            tracer,
            new ReadRecordsResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()), tracer, retry, lastResult),
            cb);
    }
}
