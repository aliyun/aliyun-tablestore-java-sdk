package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.PutTimeseriesDataResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.TimeseriesProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.timeseries.Timeseries;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataRequest;
import com.alicloud.openservices.tablestore.model.timeseries.PutTimeseriesDataResponse;
import com.google.common.cache.Cache;
import org.apache.http.concurrent.FutureCallback;

public class PutTimeseriesDataLauncher extends OperationLauncher<PutTimeseriesDataRequest, PutTimeseriesDataResponse> {

    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;
    private Cache<String, Long> timeseriesMetaCache;

    public PutTimeseriesDataLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            PutTimeseriesDataRequest originRequest,
            Cache<String, Long> timeseriesMetaCache) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);
        Preconditions.checkNotNull(timeseriesMetaCache);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
        this.timeseriesMetaCache = timeseriesMetaCache;
    }

    @Override
    public PutTimeseriesDataRequest getRequestForRetry(Exception e) {
        PutTimeseriesDataRequest request = this.originRequest;
        if (e instanceof PartialResultFailedException) {
            lastResult = (PutTimeseriesDataResponse) ((PartialResultFailedException) e).getResult();
            request = this.originRequest.createRequestForRetry(
                    lastResult.getFailedRows());
        }
        return request;
    }

    @Override
    public void fire(PutTimeseriesDataRequest request, FutureCallback<PutTimeseriesDataResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        Timeseries.PutTimeseriesDataResponse defaultResponse =
            Timeseries.PutTimeseriesDataResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            TimeseriesProtocolBuilder.buildPutTimeseriesDataRequest(request, timeseriesMetaCache),
            tracer,
            new PutTimeseriesDataResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult, request, timeseriesMetaCache),
            cb);
    }
}
