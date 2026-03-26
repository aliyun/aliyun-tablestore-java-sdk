package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListDocumentsRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListDocumentsResponse;

public class ListDocumentsLauncher extends AbstractKnowledgeBaseLauncher<ListDocumentsRequest, ListDocumentsResponse> {

    public ListDocumentsLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            ListDocumentsRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(ListDocumentsRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<ListDocumentsResponse> getResponseClass() {
        return ListDocumentsResponse.class;
    }
}
