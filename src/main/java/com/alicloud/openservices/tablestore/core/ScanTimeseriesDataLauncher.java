package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.ScanTimeseriesDataResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.ScanTimeseriesDataResponse;
import org.apache.http.concurrent.FutureCallback;

public class ScanTimeseriesDataLauncher extends OperationLauncher<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public ScanTimeseriesDataLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            ScanTimeseriesDataRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(ScanTimeseriesDataRequest request, FutureCallback<ScanTimeseriesDataResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.ScanTimeseriesDataResponse defaultResponse =
                Timeseries.ScanTimeseriesDataResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildScanTimeseriesDataRequest(request),
                tracer,
                new ScanTimeseriesDataResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
