package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsDelivery;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.delivery.CreateDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class CreateDeliveryTaskResponseConsumer
        extends ResponseConsumer<CreateDeliveryTaskResponse> {

    public CreateDeliveryTaskResponseConsumer(
            ResultParser resultParser, TraceLogger traceLogger,
            RetryStrategy retry, CreateDeliveryTaskResponse lastResult) {
        super(resultParser, traceLogger, retry, lastResult);
    }

    @Override
    protected CreateDeliveryTaskResponse parseResult() throws Exception {
        ResponseContentWithMeta responseContent = getResponseContentWithMeta();
        OtsDelivery.CreateDeliveryTaskResponse internalResponse =
                (OtsDelivery.CreateDeliveryTaskResponse) responseContent.getMessage();
        CreateDeliveryTaskResponse response = ResponseFactory.createDeliveryTaskResponse(
                responseContent, internalResponse);
        return response;
    }
}
