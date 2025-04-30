package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.ComputeSplitsResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ComputeSplitsRequest;
import com.alicloud.openservices.tablestore.model.ComputeSplitsResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class ComputeSplitsLauncher extends OperationLauncher<ComputeSplitsRequest, ComputeSplitsResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public ComputeSplitsLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        ComputeSplitsRequest originRequest)
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
    public void fire(ComputeSplitsRequest req, FutureCallback<ComputeSplitsResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsInternalApi.ComputeSplitsResponse defaultResponse = OtsInternalApi.ComputeSplitsResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            OTSProtocolBuilder.buildComputeSplitsRequest(req),
            tracer,
            new ComputeSplitsResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}

