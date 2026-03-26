package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.UpdateKnowledgeBaseRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.UpdateKnowledgeBaseResponse;

public class UpdateKnowledgeBaseLauncher extends AbstractKnowledgeBaseLauncher<UpdateKnowledgeBaseRequest, UpdateKnowledgeBaseResponse> {

    public UpdateKnowledgeBaseLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            UpdateKnowledgeBaseRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(UpdateKnowledgeBaseRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<UpdateKnowledgeBaseResponse> getResponseClass() {
        return UpdateKnowledgeBaseResponse.class;
    }
}
