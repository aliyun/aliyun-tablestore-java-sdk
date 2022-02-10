package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.DeleteTimeseriesTableResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesTableRequest;
import com.alicloud.openservices.tablestore.model.timeseries.DeleteTimeseriesTableResponse;
import org.apache.http.concurrent.FutureCallback;

public class DeleteTimeseriesTableLauncher extends OperationLauncher<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DeleteTimeseriesTableLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        DeleteTimeseriesTableRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(DeleteTimeseriesTableRequest request, FutureCallback<DeleteTimeseriesTableResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.DeleteTimeseriesTableResponse defaultResponse =
            Timeseries.DeleteTimeseriesTableResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            TimeseriesProtocolBuilder.buildDeleteTimeseriesTableRequest(request),
            tracer,
            new DeleteTimeseriesTableResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
