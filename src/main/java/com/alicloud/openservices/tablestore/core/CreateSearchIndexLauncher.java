package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.CreateSearchIndexResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.*;
import com.alicloud.openservices.tablestore.model.search.CreateSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.CreateSearchIndexRequest;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class CreateSearchIndexLauncher
        extends OperationLauncher<CreateSearchIndexRequest, CreateSearchIndexResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public CreateSearchIndexLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            CreateSearchIndexRequest originRequest)
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
    public void fire(CreateSearchIndexRequest req, FutureCallback<CreateSearchIndexResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Search.CreateSearchIndexResponse defaultResponse =
                Search.CreateSearchIndexResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                SearchProtocolBuilder.buildCreateSearchIndexRequest(req),
                tracer,
                new CreateSearchIndexResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}

