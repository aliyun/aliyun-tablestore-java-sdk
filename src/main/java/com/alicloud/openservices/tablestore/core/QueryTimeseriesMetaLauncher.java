package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.QueryTimeseriesMetaResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.QueryTimeseriesMetaRequest;
import com.alicloud.openservices.tablestore.model.timeseries.QueryTimeseriesMetaResponse;
import org.apache.http.concurrent.FutureCallback;

public class QueryTimeseriesMetaLauncher extends OperationLauncher<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public QueryTimeseriesMetaLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        QueryTimeseriesMetaRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(QueryTimeseriesMetaRequest request, FutureCallback<QueryTimeseriesMetaResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.QueryTimeseriesMetaResponse defaultResponse =
            Timeseries.QueryTimeseriesMetaResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            TimeseriesProtocolBuilder.buildQueryTimeseriesMetaRequest(request),
            tracer,
            new QueryTimeseriesMetaResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
