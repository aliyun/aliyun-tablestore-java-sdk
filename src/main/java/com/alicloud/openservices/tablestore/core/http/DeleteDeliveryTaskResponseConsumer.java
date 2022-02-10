package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsDelivery;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.delivery.DeleteDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DeleteDeliveryTaskResponseConsumer
        extends ResponseConsumer<DeleteDeliveryTaskResponse> {

    public DeleteDeliveryTaskResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, DeleteDeliveryTaskResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected DeleteDeliveryTaskResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsDelivery.DeleteDeliveryTaskResponse internalResponse =
                (OtsDelivery.DeleteDeliveryTaskResponse) responseContent.getMessage();
        DeleteDeliveryTaskResponse response = ResponseFactory.deleteDeliveryTaskResponse(
                responseContent, internalResponse);
        return response;
    }
}