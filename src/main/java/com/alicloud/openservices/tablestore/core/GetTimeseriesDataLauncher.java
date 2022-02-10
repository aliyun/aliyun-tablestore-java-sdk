package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.GetTimeseriesDataResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.GetTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.GetTimeseriesDataResponse;
import org.apache.http.concurrent.FutureCallback;

public class GetTimeseriesDataLauncher extends OperationLauncher<GetTimeseriesDataRequest, GetTimeseriesDataResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public GetTimeseriesDataLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        GetTimeseriesDataRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(GetTimeseriesDataRequest request, FutureCallback<GetTimeseriesDataResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.GetTimeseriesDataResponse defaultResponse =
            Timeseries.GetTimeseriesDataResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            TimeseriesProtocolBuilder.buildGetTimeseriesDataRequest(request),
            tracer,
            new GetTimeseriesDataResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
