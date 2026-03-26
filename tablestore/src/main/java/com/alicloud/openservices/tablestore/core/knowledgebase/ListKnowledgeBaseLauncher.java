package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListKnowledgeBaseRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListKnowledgeBaseResponse;

public class ListKnowledgeBaseLauncher extends AbstractKnowledgeBaseLauncher<ListKnowledgeBaseRequest, ListKnowledgeBaseResponse> {

    public ListKnowledgeBaseLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            ListKnowledgeBaseRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(ListKnowledgeBaseRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<ListKnowledgeBaseResponse> getResponseClass() {
        return ListKnowledgeBaseResponse.class;
    }
}
