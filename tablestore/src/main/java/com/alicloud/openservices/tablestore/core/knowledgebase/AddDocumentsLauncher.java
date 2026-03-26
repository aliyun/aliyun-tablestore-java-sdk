package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.AddDocumentsRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.AddDocumentsResponse;

public class AddDocumentsLauncher extends AbstractKnowledgeBaseLauncher<AddDocumentsRequest, AddDocumentsResponse> {

    public AddDocumentsLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            AddDocumentsRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(AddDocumentsRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<AddDocumentsResponse> getResponseClass() {
        return AddDocumentsResponse.class;
    }
}
