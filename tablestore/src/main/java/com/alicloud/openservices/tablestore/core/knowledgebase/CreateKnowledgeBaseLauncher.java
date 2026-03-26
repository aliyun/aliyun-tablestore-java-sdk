package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.CreateKnowledgeBaseRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.CreateKnowledgeBaseResponse;

public class CreateKnowledgeBaseLauncher extends AbstractKnowledgeBaseLauncher<CreateKnowledgeBaseRequest, CreateKnowledgeBaseResponse> {

    public CreateKnowledgeBaseLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            CreateKnowledgeBaseRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(CreateKnowledgeBaseRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<CreateKnowledgeBaseResponse> getResponseClass() {
        return CreateKnowledgeBaseResponse.class;
    }
}
