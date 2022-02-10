package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.UpdateTimeseriesMetaResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.UpdateTimeseriesMetaRequest;
import com.alicloud.openservices.tablestore.model.timeseries.UpdateTimeseriesMetaResponse;
import org.apache.http.concurrent.FutureCallback;

public class UpdateTimeseriesMetaLauncher extends OperationLauncher<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public UpdateTimeseriesMetaLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            UpdateTimeseriesMetaRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(UpdateTimeseriesMetaRequest request, FutureCallback<UpdateTimeseriesMetaResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.UpdateTimeseriesMetaResponse defaultResponse =
                Timeseries.UpdateTimeseriesMetaResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildUpdateTimeseriesMetaRequest(request),
                tracer,
                new UpdateTimeseriesMetaResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
