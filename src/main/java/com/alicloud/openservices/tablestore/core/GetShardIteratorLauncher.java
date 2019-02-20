package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.GetShardIteratorResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.GetShardIteratorRequest;
import com.alicloud.openservices.tablestore.model.GetShardIteratorResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import org.apache.http.concurrent.FutureCallback;

public class GetShardIteratorLauncher
        extends OperationLauncher<GetShardIteratorRequest, GetShardIteratorResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public GetShardIteratorLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            GetShardIteratorRequest originRequest)
    {
        super(instanceName, client, crdsProvider, config, originRequest);

        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(GetShardIteratorRequest req, FutureCallback<GetShardIteratorResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsInternalApi.GetShardIteratorResponse defaultResponse =
                OtsInternalApi.GetShardIteratorResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                OTSProtocolBuilder.buildGetShardIteratorRequest(req),
                tracer,
                new GetShardIteratorResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}

