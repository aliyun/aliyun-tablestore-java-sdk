package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.ListDeliveryTaskResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.OtsDelivery;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.delivery.ListDeliveryTaskRequest;
import com.alicloud.openservices.tablestore.model.delivery.ListDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import org.apache.http.concurrent.FutureCallback;

public class ListDeliveryTaskLauncher
        extends OperationLauncher<ListDeliveryTaskRequest, ListDeliveryTaskResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public ListDeliveryTaskLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            ListDeliveryTaskRequest originRequest)
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
    public void fire(ListDeliveryTaskRequest req, FutureCallback<ListDeliveryTaskResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsDelivery.ListDeliveryTaskResponse defaultResponse =
                OtsDelivery.ListDeliveryTaskResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                OTSProtocolBuilder.buildListDeliveryTaskRequest(req),
                tracer,
                new ListDeliveryTaskResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}