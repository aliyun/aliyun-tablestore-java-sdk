package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.DescribeTimeseriesAnalyticalStoreResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DescribeTimeseriesAnalyticalStoreRequest;
import com.alicloud.openservices.tablestore.model.timeseries.DescribeTimeseriesAnalyticalStoreResponse;
import com.alicloud.openservices.tablestore.timeline.utils.Preconditions;
import org.apache.http.concurrent.FutureCallback;

public class DescribeTimeseriesAnalyticalStoreLauncher extends OperationLauncher<DescribeTimeseriesAnalyticalStoreRequest, DescribeTimeseriesAnalyticalStoreResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DescribeTimeseriesAnalyticalStoreLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DescribeTimeseriesAnalyticalStoreRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);
        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(DescribeTimeseriesAnalyticalStoreRequest request, FutureCallback<DescribeTimeseriesAnalyticalStoreResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.DescribeTimeseriesAnalyticalStoreResponse defaultResponse =
                Timeseries.DescribeTimeseriesAnalyticalStoreResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                TimeseriesProtocolBuilder.buildDescribeTimeseriesAnalyticalStoreRequest(request),
                tracer,
                new DescribeTimeseriesAnalyticalStoreResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
