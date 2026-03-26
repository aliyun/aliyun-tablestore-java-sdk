package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.RetrieveRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.RetrieveResponse;

public class RetrieveLauncher extends AbstractKnowledgeBaseLauncher<RetrieveRequest, RetrieveResponse> {

    public RetrieveLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            RetrieveRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(RetrieveRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<RetrieveResponse> getResponseClass() {
        return RetrieveResponse.class;
    }
}
