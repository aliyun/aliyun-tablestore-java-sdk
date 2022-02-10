package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.DescribeTimeseriesTableResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.DescribeTimeseriesTableRequest;
import com.alicloud.openservices.tablestore.model.timeseries.DescribeTimeseriesTableResponse;
import org.apache.http.concurrent.FutureCallback;

public class DescribeTimeseriesTableLauncher extends OperationLauncher<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public DescribeTimeseriesTableLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        DescribeTimeseriesTableRequest originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public void fire(DescribeTimeseriesTableRequest request, FutureCallback<DescribeTimeseriesTableResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.DescribeTimeseriesTableResponse defaultResponse =
            Timeseries.DescribeTimeseriesTableResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            TimeseriesProtocolBuilder.buildDescribeTimeseriesTableRequest(request),
            tracer,
            new DescribeTimeseriesTableResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
