package com.alicloud.openservices.tablestore.core.memory;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.knowledgebase.AbstractKnowledgeBaseLauncher;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.memory.MemoryRequest;

import java.nio.charset.StandardCharsets;

public class JsonOperationLauncher<Req extends MemoryRequest, Res extends Response>
        extends AbstractKnowledgeBaseLauncher<Req, Res> {
    private final Class<Res> responseClass;

    public JsonOperationLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            Req originRequest,
            Class<Res> responseClass) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
        this.responseClass = Preconditions.checkNotNull(responseClass, "Response class cannot be null");
    }

    @Override
    protected String toJson(Req req) {
        return req.toJson();
    }

    @Override
    protected String formatRequestMessageForLog(String message) {
        int size = message == null ? 0 : message.getBytes(StandardCharsets.UTF_8).length;
        return "<redacted: " + size + " bytes>";
    }

    @Override
    public Class<Res> getResponseClass() {
        return responseClass;
    }

    public OTSUri getUri() {
        return uri;
    }
}
