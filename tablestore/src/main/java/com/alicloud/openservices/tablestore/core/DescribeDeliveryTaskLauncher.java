package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.DescribeDeliveryTaskResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.OtsDelivery;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.delivery.DescribeDeliveryTaskRequest;
import com.alicloud.openservices.tablestore.model.delivery.DescribeDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class DescribeDeliveryTaskLauncher
        extends OperationLauncher<DescribeDeliveryTaskRequest, DescribeDeliveryTaskResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DescribeDeliveryTaskLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DescribeDeliveryTaskRequest originRequest)
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
    public void fire(DescribeDeliveryTaskRequest req, FutureCallback<DescribeDeliveryTaskResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsDelivery.DescribeDeliveryTaskResponse defaultResponse =
                OtsDelivery.DescribeDeliveryTaskResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                OTSProtocolBuilder.buildDescribeDeliveryTaskRequest(req),
                tracer,
                new DescribeDeliveryTaskResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
