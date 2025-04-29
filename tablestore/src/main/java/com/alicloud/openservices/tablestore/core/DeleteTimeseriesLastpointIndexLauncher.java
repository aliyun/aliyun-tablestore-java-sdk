package com.alicloud.openservices.tablestore.core;


import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.DeleteTimeseriesLastpointIndexResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesLastpointIndexRequest;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesLastpointIndexResponse;
import com.alicloud.openservices.tablestore.timeline.utils.Preconditions;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

public class DeleteTimeseriesLastpointIndexLauncher extends OperationLauncher<DeleteTimeseriesLastpointIndexRequest, DeleteTimeseriesLastpointIndexResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DeleteTimeseriesLastpointIndexLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DeleteTimeseriesLastpointIndexRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);
        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(DeleteTimeseriesLastpointIndexRequest request, FutureCallback<DeleteTimeseriesLastpointIndexResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.DeleteTimeseriesLastpointIndexResponse defaultResponse =
                Timeseries.DeleteTimeseriesLastpointIndexResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildDeleteTimeseriesLastpointIndexRequest(request),
                tracer,
                new DeleteTimeseriesLastpointIndexResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
