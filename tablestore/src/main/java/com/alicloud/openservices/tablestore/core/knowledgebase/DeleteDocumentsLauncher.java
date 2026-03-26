package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.DeleteDocumentsRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.DeleteDocumentsResponse;

public class DeleteDocumentsLauncher extends AbstractKnowledgeBaseLauncher<DeleteDocumentsRequest, DeleteDocumentsResponse> {

    public DeleteDocumentsLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DeleteDocumentsRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(DeleteDocumentsRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<DeleteDocumentsResponse> getResponseClass() {
        return DeleteDocumentsResponse.class;
    }
}
