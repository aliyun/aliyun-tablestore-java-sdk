package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.CreateTimeseriesAnalyticalStoreResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesAnalyticalStoreRequest;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesAnalyticalStoreResponse;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class CreateTimeseriesAnalyticalStoreLauncher extends OperationLauncher<CreateTimeseriesAnalyticalStoreRequest, CreateTimeseriesAnalyticalStoreResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public CreateTimeseriesAnalyticalStoreLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            CreateTimeseriesAnalyticalStoreRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(CreateTimeseriesAnalyticalStoreRequest request, FutureCallback<CreateTimeseriesAnalyticalStoreResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.CreateTimeseriesAnalyticalStoreResponse defaultResponse =
                Timeseries.CreateTimeseriesAnalyticalStoreResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildCreateTimeseriesAnalyticalStoreRequest(request),
                tracer,
                new CreateTimeseriesAnalyticalStoreResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
