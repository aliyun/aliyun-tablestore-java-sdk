package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.UpdateSearchIndexResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.core.protocol.SearchProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.search.UpdateSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.UpdateSearchIndexResponse;
import org.apache.http.concurrent.FutureCallback;

public class UpdateSearchIndexLauncher
    extends OperationLauncher<UpdateSearchIndexRequest, UpdateSearchIndexResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public UpdateSearchIndexLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            UpdateSearchIndexRequest originRequest)
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
    public void fire(UpdateSearchIndexRequest request, FutureCallback<UpdateSearchIndexResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Search.UpdateSearchIndexResponse defaultResponse =
                Search.UpdateSearchIndexResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                SearchProtocolBuilder.buildUpdateSearchIndexRequest(request),
                tracer,
                new UpdateSearchIndexResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
