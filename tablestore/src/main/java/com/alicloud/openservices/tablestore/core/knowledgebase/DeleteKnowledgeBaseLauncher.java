package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.DeleteKnowledgeBaseRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.DeleteKnowledgeBaseResponse;

public class DeleteKnowledgeBaseLauncher extends AbstractKnowledgeBaseLauncher<DeleteKnowledgeBaseRequest, DeleteKnowledgeBaseResponse> {

    public DeleteKnowledgeBaseLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DeleteKnowledgeBaseRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(DeleteKnowledgeBaseRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<DeleteKnowledgeBaseResponse> getResponseClass() {
        return DeleteKnowledgeBaseResponse.class;
    }
}
