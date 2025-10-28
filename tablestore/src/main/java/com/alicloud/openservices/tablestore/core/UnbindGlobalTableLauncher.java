package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.UnbindGlobalTableResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTable;
import com.alicloud.openservices.tablestore.core.protocol.globaltable.GlobalTableProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.UnbindGlobalTableRequest;
import com.alicloud.openservices.tablestore.model.UnbindGlobalTableResponse;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class UnbindGlobalTableLauncher extends OperationLauncher<UnbindGlobalTableRequest, UnbindGlobalTableResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public UnbindGlobalTableLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        UnbindGlobalTableRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(UnbindGlobalTableRequest request, FutureCallback<UnbindGlobalTableResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        GlobalTable.UnbindGlobalTableResponse defaultResponse =
                GlobalTable.UnbindGlobalTableResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            GlobalTableProtocolBuilder.buildUnbindGlobalTableRequest(request),
            tracer,
            new UnbindGlobalTableResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
