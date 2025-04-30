package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.BulkImportResponseConsumer;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;

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

public class BulkImportLauncher extends OperationLauncher<BulkImportRequest, BulkImportResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public BulkImportLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            BulkImportRequest originRequest)
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
    public BulkImportRequest getRequestForRetry(Exception e) {
        BulkImportRequest request = this.originRequest;
        if (e instanceof PartialResultFailedException) {
            lastResult = (BulkImportResponse) ((PartialResultFailedException) e).getResult();  //???
            request = this.originRequest.createRequestForRetry(
                    lastResult.getFailedRows());
        }
        return request;
    }

    @Override
    public void fire(BulkImportRequest req, FutureCallback<BulkImportResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsInternalApi.BulkImportResponse defaultResponse =
                OtsInternalApi.BulkImportResponse.getDefaultInstance();
        asyncInvokePost(
                uri,
                null,
                OTSProtocolBuilder.buildBulkImportRequest(req),
                tracer,
                new BulkImportResponseConsumer(
                        ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                defaultResponse, tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }
}
