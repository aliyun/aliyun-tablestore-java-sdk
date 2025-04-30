package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.CreateTimeseriesTableResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesTableRequest;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesTableResponse;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class CreateTimeseriesTableLauncher extends OperationLauncher<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public CreateTimeseriesTableLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        CreateTimeseriesTableRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(CreateTimeseriesTableRequest request, FutureCallback<CreateTimeseriesTableResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.CreateTimeseriesTableResponse defaultResponse =
            Timeseries.CreateTimeseriesTableResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            TimeseriesProtocolBuilder.buildCreateTimeseriesTableRequest(request),
            tracer,
            new CreateTimeseriesTableResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
