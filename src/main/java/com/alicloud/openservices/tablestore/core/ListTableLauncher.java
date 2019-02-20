package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.ListTableResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.ListTableRequest;
import com.alicloud.openservices.tablestore.model.ListTableResponse;

public class ListTableLauncher
  extends OperationLauncher<ListTableRequest, ListTableResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public ListTableLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        ListTableRequest originRequest)
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
    public void fire(ListTableRequest req, FutureCallback<ListTableResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsInternalApi.ListTableResponse defaultResponse =
            OtsInternalApi.ListTableResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            OTSProtocolBuilder.buildListTableRequest(),
            tracer,
            new ListTableResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}

