package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.UpdateTimeseriesAnalyticalStoreResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.UpdateTimeseriesAnalyticalStoreRequest;
import com.alicloud.openservices.tablestore.model.timeseries.UpdateTimeseriesAnalyticalStoreResponse;
import com.alicloud.openservices.tablestore.timeline.utils.Preconditions;
import org.apache.http.concurrent.FutureCallback;

public class UpdateTimeseriesAnalyticalStoreLauncher extends OperationLauncher<UpdateTimeseriesAnalyticalStoreRequest, UpdateTimeseriesAnalyticalStoreResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public UpdateTimeseriesAnalyticalStoreLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            UpdateTimeseriesAnalyticalStoreRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);
        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(UpdateTimeseriesAnalyticalStoreRequest request, FutureCallback<UpdateTimeseriesAnalyticalStoreResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.UpdateTimeseriesAnalyticalStoreResponse defaultResponse =
                Timeseries.UpdateTimeseriesAnalyticalStoreResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildUpdateTimeseriesAnalyticalStoreRequest(request),
                tracer,
                new UpdateTimeseriesAnalyticalStoreResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
