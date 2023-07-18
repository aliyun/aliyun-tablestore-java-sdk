package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.DeleteTimeseriesAnalyticalStoreResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesAnalyticalStoreRequest;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesAnalyticalStoreResponse;
import org.apache.http.concurrent.FutureCallback;

public class DeleteTimeseriesAnalyticalStoreLauncher extends OperationLauncher<DeleteTimeseriesAnalyticalStoreRequest, DeleteTimeseriesAnalyticalStoreResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DeleteTimeseriesAnalyticalStoreLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DeleteTimeseriesAnalyticalStoreRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(DeleteTimeseriesAnalyticalStoreRequest request, FutureCallback<DeleteTimeseriesAnalyticalStoreResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.DeleteTimeseriesAnalyticalStoreResponse defaultResponse =
                Timeseries.DeleteTimeseriesAnalyticalStoreResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildDeleteTimeseriesAnalyticalStoreRequest(request),
                tracer,
                new DeleteTimeseriesAnalyticalStoreResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
