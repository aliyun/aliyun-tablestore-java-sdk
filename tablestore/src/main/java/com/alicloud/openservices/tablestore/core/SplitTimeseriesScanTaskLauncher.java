package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.SplitTimeseriesScanTaskResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.SplitTimeseriesScanTaskRequest;
import com.alicloud.openservices.tablestore.model.timeseries.SplitTimeseriesScanTaskResponse;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class SplitTimeseriesScanTaskLauncher extends OperationLauncher<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public SplitTimeseriesScanTaskLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            SplitTimeseriesScanTaskRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(SplitTimeseriesScanTaskRequest request, FutureCallback<SplitTimeseriesScanTaskResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.SplitTimeseriesScanTaskResponse defaultResponse =
                Timeseries.SplitTimeseriesScanTaskResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildSplitTimeseriesScanTaskRequest(request),
                tracer,
                new SplitTimeseriesScanTaskResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
