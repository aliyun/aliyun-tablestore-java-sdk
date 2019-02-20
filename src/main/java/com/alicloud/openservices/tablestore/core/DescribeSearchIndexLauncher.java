package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.DescribeSearchIndexResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.*;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexResponse;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DescribeSearchIndexLauncher
        extends OperationLauncher<DescribeSearchIndexRequest, DescribeSearchIndexResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DescribeSearchIndexLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DescribeSearchIndexRequest originRequest)
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
    public void fire(DescribeSearchIndexRequest req, FutureCallback<DescribeSearchIndexResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Search.DescribeSearchIndexResponse defaultResponse =
                Search.DescribeSearchIndexResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                SearchProtocolBuilder.buildDescribeSearchIndexRequest(req),
                tracer,
                new DescribeSearchIndexResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}

