package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;

public class SQLQueryResponseConsumer extends ResponseConsumer<SQLQueryResponse> {
    public SQLQueryResponseConsumer(ResultParser resultParser, TraceLogger traceLogger, RetryStrategy retry, SQLQueryResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected SQLQueryResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsInternalApi.SQLQueryResponse internalResponse = (OtsInternalApi.SQLQueryResponse) responseContent.getMessage();
        return ResponseFactory.createSqlQueryResponse(responseContent, internalResponse);
    }
}
