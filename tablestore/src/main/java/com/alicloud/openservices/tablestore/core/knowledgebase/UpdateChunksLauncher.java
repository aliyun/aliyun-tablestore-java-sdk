package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.UpdateChunksRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.UpdateChunksResponse;

public class UpdateChunksLauncher extends AbstractKnowledgeBaseLauncher<UpdateChunksRequest, UpdateChunksResponse> {

    public UpdateChunksLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            UpdateChunksRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(UpdateChunksRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<UpdateChunksResponse> getResponseClass() {
        return UpdateChunksResponse.class;
    }
}
