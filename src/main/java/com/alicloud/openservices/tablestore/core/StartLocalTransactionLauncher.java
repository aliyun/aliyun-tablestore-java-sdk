package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.StartLocalTransactionResponseConsumer;
import com.alicloud.openservices.tablestore.model.*;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;

public class StartLocalTransactionLauncher
        extends OperationLauncher<StartLocalTransactionRequest, StartLocalTransactionResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public StartLocalTransactionLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            StartLocalTransactionRequest originRequest)
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
    public void fire(StartLocalTransactionRequest req, FutureCallback<StartLocalTransactionResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsInternalApi.StartLocalTransactionResponse defaultResponse =
                OtsInternalApi.StartLocalTransactionResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                OTSProtocolBuilder.buildStartLocalTransactionRequest(req),
                tracer,
                new StartLocalTransactionResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}

