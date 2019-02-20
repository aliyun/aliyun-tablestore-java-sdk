package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.ListSearchIndexResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.*;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexResponse;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class ListSearchIndexLauncher
        extends OperationLauncher<ListSearchIndexRequest, ListSearchIndexResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public ListSearchIndexLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            ListSearchIndexRequest originRequest)
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
    public void fire(ListSearchIndexRequest req, FutureCallback<ListSearchIndexResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Search.ListSearchIndexResponse defaultResponse =
                Search.ListSearchIndexResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                SearchProtocolBuilder.buildListSearchIndexRequest(req),
                tracer,
                new ListSearchIndexResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}

