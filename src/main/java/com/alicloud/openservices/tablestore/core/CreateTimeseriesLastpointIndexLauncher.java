package com.alicloud.openservices.tablestore.core;


import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.CreateTimeseriesLastpointIndexResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesLastpointIndexRequest;
import com.alicloud.openservices.tablestore.model.timeseries.CreateTimeseriesLastpointIndexResponse;
import com.alicloud.openservices.tablestore.timeline.utils.Preconditions;
import org.apache.http.concurrent.FutureCallback;

public class CreateTimeseriesLastpointIndexLauncher extends OperationLauncher<CreateTimeseriesLastpointIndexRequest, CreateTimeseriesLastpointIndexResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public CreateTimeseriesLastpointIndexLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            CreateTimeseriesLastpointIndexRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);
        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(CreateTimeseriesLastpointIndexRequest request, FutureCallback<CreateTimeseriesLastpointIndexResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.CreateTimeseriesLastpointIndexResponse defaultResponse =
                Timeseries.CreateTimeseriesLastpointIndexResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildCreateTimeseriesLastpointIndexRequest(request),
                tracer,
                new CreateTimeseriesLastpointIndexResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
