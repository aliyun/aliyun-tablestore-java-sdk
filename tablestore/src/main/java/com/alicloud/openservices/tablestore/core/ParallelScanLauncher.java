package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.ParallelScanResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.Search;
import com.alicloud.openservices.tablestore.core.protocol.SearchProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.ParallelScanResponse;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class ParallelScanLauncher extends OperationLauncher<ParallelScanRequest, ParallelScanResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public ParallelScanLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        ParallelScanRequest originRequest)
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
    public void fire(ParallelScanRequest req, FutureCallback<ParallelScanResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Search.ParallelScanResponse defaultResponse = Search.ParallelScanResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            SearchProtocolBuilder.buildParallelScanRequest(req),
            tracer,
            new ParallelScanResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}

