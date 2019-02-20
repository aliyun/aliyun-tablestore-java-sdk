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
import com.alicloud.openservices.tablestore.core.http.BatchGetRowResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;

public class BatchGetRowLauncher extends OperationLauncher<BatchGetRowRequest, BatchGetRowResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public BatchGetRowLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        BatchGetRowRequest originRequest)
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
    public BatchGetRowRequest getRequestForRetry(Exception e) {
    	BatchGetRowRequest request = this.originRequest;
        if (e instanceof PartialResultFailedException) {
            lastResult = (BatchGetRowResponse) ((PartialResultFailedException) e).getResult();
            request = this.originRequest.createRequestForRetry(lastResult.getFailedRows());
        }
    	return request;
    }

    @Override
    public void fire(BatchGetRowRequest req, FutureCallback<BatchGetRowResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsInternalApi.BatchGetRowResponse defaultResponse =
            OtsInternalApi.BatchGetRowResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            OTSProtocolBuilder.buildBatchGetRowRequest(req.getCriteriasByTable()),
            tracer,
            new BatchGetRowResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
    }
}
