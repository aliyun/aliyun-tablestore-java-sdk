package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.DescribeGlobalTableResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTable;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTableProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.DescribeGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeGlobalTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class DescribeGlobalTableLauncher extends OperationLauncher<DescribeGlobalTableRequest, DescribeGlobalTableResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DescribeGlobalTableLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        DescribeGlobalTableRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(DescribeGlobalTableRequest request, FutureCallback<DescribeGlobalTableResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        GlobalTable.DescribeGlobalTableResponse defaultResponse =
                GlobalTable.DescribeGlobalTableResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            GlobalTableProtocolBuilder.buildDescribeGlobalTableRequest(request),
            tracer,
            new DescribeGlobalTableResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
