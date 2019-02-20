package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.DeleteSearchIndexResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.*;
import com.alicloud.openservices.tablestore.model.search.DeleteSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DeleteSearchIndexResponse;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DeleteSearchIndexLauncher
        extends OperationLauncher<DeleteSearchIndexRequest, DeleteSearchIndexResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DeleteSearchIndexLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DeleteSearchIndexRequest originRequest)
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
    public void fire(DeleteSearchIndexRequest req, FutureCallback<DeleteSearchIndexResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Search.DeleteSearchIndexResponse defaultResponse =
                Search.DeleteSearchIndexResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                SearchProtocolBuilder.buildDeleteSearchIndexRequest(req),
                tracer,
                new DeleteSearchIndexResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}

