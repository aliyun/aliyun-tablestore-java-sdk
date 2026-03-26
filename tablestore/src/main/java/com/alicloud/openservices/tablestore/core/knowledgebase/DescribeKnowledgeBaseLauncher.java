package com.alicloud.openservices.tablestore.core.knowledgebase;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.knowledgebase.DescribeKnowledgeBaseRequest;
import com.alicloud.openservices.tablestore.model.knowledgebase.DescribeKnowledgeBaseResponse;

public class DescribeKnowledgeBaseLauncher extends AbstractKnowledgeBaseLauncher<DescribeKnowledgeBaseRequest, DescribeKnowledgeBaseResponse> {

    public DescribeKnowledgeBaseLauncher(
            OTSUri uri,
            TraceLogger tracer,
            RetryStrategy retry,
            String instanceName,
            AsyncServiceClient client,
            CredentialsProvider crdsProvider,
            ClientConfiguration config,
            DescribeKnowledgeBaseRequest originRequest) {
        super(uri, tracer, retry, instanceName, client, crdsProvider, config, originRequest);
    }

    @Override
    protected String toJson(DescribeKnowledgeBaseRequest req) {
        return req.toJson();
    }

    @Override
    protected Class<DescribeKnowledgeBaseResponse> getResponseClass() {
        return DescribeKnowledgeBaseResponse.class;
    }
}
