package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.CreateDeliveryTaskResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.OtsDelivery;
import com.alicloud.openservices.tablestore.model.delivery.CreateDeliveryTaskRequest;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.delivery.CreateDeliveryTaskResponse;

public class CreateDeliveryTaskLauncher
        extends OperationLauncher<CreateDeliveryTaskRequest, CreateDeliveryTaskResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public CreateDeliveryTaskLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            CreateDeliveryTaskRequest originRequest)
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
    public void fire(CreateDeliveryTaskRequest req, FutureCallback<CreateDeliveryTaskResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsDelivery.CreateDeliveryTaskResponse defaultResponse =
                OtsDelivery.CreateDeliveryTaskResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                OTSProtocolBuilder.buildCreateDeliveryTaskRequest(req),
                tracer,
                new CreateDeliveryTaskResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}