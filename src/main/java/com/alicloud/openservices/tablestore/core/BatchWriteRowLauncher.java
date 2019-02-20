package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.PartialResultFailedException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.BatchWriteRowResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;

public class BatchWriteRowLauncher extends OperationLauncher<BatchWriteRowRequest, BatchWriteRowResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public BatchWriteRowLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        BatchWriteRowRequest originRequest)
    {
        super(instanceName, client, crdsProvider, config, originRequest);

        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);

        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }
    
    @Override
    public BatchWriteRowRequest getRequestForRetry(Exception e) {
    	BatchWriteRowRequest request = this.originRequest;
        if (e instanceof PartialResultFailedException) {
            lastResult = (BatchWriteRowResponse) ((PartialResultFailedException) e).getResult();
            request = this.originRequest.createRequestForRetry(
                    lastResult.getFailedRows());
        }
    	return request;
    }

    @Override
    public void fire(BatchWriteRowRequest req, FutureCallback<BatchWriteRowResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsInternalApi.BatchWriteRowResponse defaultResponse =
            OtsInternalApi.BatchWriteRowResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            OTSProtocolBuilder.buildBatchWriteRowRequest(req),
            tracer,
            new BatchWriteRowResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
