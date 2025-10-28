package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.CreateGlobalTableResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTable;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTableProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.CreateGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.CreateGlobalTableResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class CreateGlobalTableLauncher extends OperationLauncher<CreateGlobalTableRequest, CreateGlobalTableResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public CreateGlobalTableLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        CreateGlobalTableRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(CreateGlobalTableRequest request, FutureCallback<CreateGlobalTableResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        GlobalTable.CreateGlobalTableResponse defaultResponse =
                GlobalTable.CreateGlobalTableResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            GlobalTableProtocolBuilder.buildCreateGlobalTableRequest(request),
            tracer,
            new CreateGlobalTableResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
