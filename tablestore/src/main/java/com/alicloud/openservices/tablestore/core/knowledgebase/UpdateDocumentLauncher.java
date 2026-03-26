package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.UpdateDocumentRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.UpdateDocumentResponse;

public class UpdateDocumentLauncher extends AbstractKnowledgeBaseLauncher<UpdateDocumentRequest, UpdateDocumentResponse> {

    public UpdateDocumentLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            UpdateDocumentRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(UpdateDocumentRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<UpdateDocumentResponse> getResponseClass() {
        return UpdateDocumentResponse.class;
    }
}
