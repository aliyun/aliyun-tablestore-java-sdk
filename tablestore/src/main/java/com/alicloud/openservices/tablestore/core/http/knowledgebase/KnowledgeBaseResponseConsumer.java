package com.alicloud.openservices.tablestore.core.http.knowledgebase;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.http.ResponseConsumer;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class KnowledgeBaseResponseConsumer<T extends Response> extends ResponseConsumer<T> {
    private final Class<T> responseClass;

    public KnowledgeBaseResponseConsumer(
            Class<T> responseClass,
            ResultParser resultParser,
            TraceLogger traceLogger,
            RetryStrategy retry,
            T lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
        this.responseClass = responseClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T parseResult() throws Exception {
        Response response = getJsonResponseContentWithMeta();
        if (!responseClass.isInstance(response)) {
            throw new ClientException("Unexpected response type: " + response.getClass().getName());
        }
        return (T) response;
    }
}
