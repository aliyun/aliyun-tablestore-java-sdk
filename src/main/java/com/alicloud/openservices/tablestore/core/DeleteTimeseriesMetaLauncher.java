package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.DeleteTimeseriesMetaResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesMetaRequest;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesMetaResponse;
import org.apache.http.concurrent.FutureCallback;

public class DeleteTimeseriesMetaLauncher extends OperationLauncher<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DeleteTimeseriesMetaLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DeleteTimeseriesMetaRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(DeleteTimeseriesMetaRequest request, FutureCallback<DeleteTimeseriesMetaResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.DeleteTimeseriesMetaResponse defaultResponse =
                Timeseries.DeleteTimeseriesMetaResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildDeleteTimeseriesMetaRequest(request),
                tracer,
                new DeleteTimeseriesMetaResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
