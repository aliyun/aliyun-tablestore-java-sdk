package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.aliyun.ots.thirdparty.org.apache.http.concurrent.FutureCallback;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.OperationLauncher;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.knowledgebase.KnowledgeBaseResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.JsonResultParser;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public abstract class AbstractKnowledgeBaseLauncher<Req, Res extends Response>
        extends OperationLauncher<Req, Res> {

    protected final OTSUri uri;
    protected final TraceLogger tracer;
    protected final RetryStrategy retry;

    protected AbstractKnowledgeBaseLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            Req originRequest) {
        super(instanceName, client, crdsProvider, config, originRequest);
        Preconditions.checkNotNull(uri);
        Preconditions.checkNotNull(tracer);
        Preconditions.checkNotNull(retry);
        this.uri = uri;
        this.tracer = tracer;
        this.retry = retry;
    }

    @Override
    public final void fire(Req req, FutureCallback<Res> cb) {
        Preconditions.checkNotNull(req, "Request cannot be null");
        LogUtil.logBeforeExecution(tracer, retry);

        asyncInvokePost(
                uri,
                null,
                toJson(req),
                tracer,
                new KnowledgeBaseResponseConsumer<>(
                        getResponseClass(),
                        new JsonResultParser<>(getResponseClass(), tracer.getTraceId()),
                        tracer, retry, lastResult),
                cb);
    }

    protected abstract String toJson(Req req);

    protected abstract Class<Res> getResponseClass();
}
