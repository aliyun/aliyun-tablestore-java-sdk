package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.GetDocumentRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.GetDocumentResponse;

public class GetDocumentLauncher extends AbstractKnowledgeBaseLauncher<GetDocumentRequest, GetDocumentResponse> {

    public GetDocumentLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            GetDocumentRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(GetDocumentRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<GetDocumentResponse> getResponseClass() {
        return GetDocumentResponse.class;
    }
}
