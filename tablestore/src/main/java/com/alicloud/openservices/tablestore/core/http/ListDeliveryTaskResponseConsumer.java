package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsDelivery;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.delivery.ListDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class ListDeliveryTaskResponseConsumer
        extends ResponseConsumer<ListDeliveryTaskResponse> {

    public ListDeliveryTaskResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, ListDeliveryTaskResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected ListDeliveryTaskResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsDelivery.ListDeliveryTaskResponse internalResponse =
                (OtsDelivery.ListDeliveryTaskResponse) responseContent.getMessage();
        ListDeliveryTaskResponse response = ResponseFactory.listDeliveryTaskResponse(
                responseContent, internalResponse);
        return response;
    }
}
