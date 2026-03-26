package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListChunksRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.ListChunksResponse;

public class ListChunksLauncher extends AbstractKnowledgeBaseLauncher<ListChunksRequest, ListChunksResponse> {

    public ListChunksLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            ListChunksRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(ListChunksRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<ListChunksResponse> getResponseClass() {
        return ListChunksResponse.class;
    }
}
