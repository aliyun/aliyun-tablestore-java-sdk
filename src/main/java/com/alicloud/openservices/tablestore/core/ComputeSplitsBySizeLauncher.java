package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.ComputeSplitsBySizeResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.GetRangeResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.ComputeSplitsBySizeRequest;
import com.alicloud.openservices.tablestore.model.ComputeSplitsBySizeResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class ComputeSplitsBySizeLauncher extends OperationLauncher<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse> {
    private OTSUri uri;
    private TraceLogger tracer;
    private RetryStrategy retry;

    public ComputeSplitsBySizeLauncher(
        OTSUri uri,
        TraceLogger tracer,
        RetryStrategy retry,
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        ComputeSplitsBySizeRequest originRequest)
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
    public void fire(ComputeSplitsBySizeRequest request, FutureCallback<ComputeSplitsBySizeResponse> cb) {
        LogUtil.logBeforeExecution(tracer, retry);

        OtsInternalApi.ComputeSplitPointsBySizeResponse defaultResponse =
            OtsInternalApi.ComputeSplitPointsBySizeResponse.getDefaultInstance();
        asyncInvokePost(
            uri,
            null,
            OTSProtocolBuilder.buildComputeSplitsBySizeRequest(request),
            tracer,
            new ComputeSplitsBySizeResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
        
    }

}
