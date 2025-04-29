package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.protocol.OtsDelivery;
import com.alicloud.openservices.tablestore.core.protocol.ResponseFactory;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.delivery.DescribeDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public class DescribeDeliveryTaskResponseConsumer
        extends ResponseConsumer<DescribeDeliveryTaskResponse> {

    public DescribeDeliveryTaskResponseConsumer(
                ResultParser resultParser, TraceLogger traceLogger,
                RetryStrategy retry, DescribeDeliveryTaskResponse lastResult) {
            super(resultParser, traceLogger, retry, lastResult);
        }

        @Override
        protected DescribeDeliveryTaskResponse parseResult() throws Exception {
            ResponseContentWithMeta responseContent = getResponseContentWithMeta();
            OtsDelivery.DescribeDeliveryTaskResponse internalResponse =
                    (OtsDelivery.DescribeDeliveryTaskResponse) responseContent.getMessage();
            DescribeDeliveryTaskResponse response = ResponseFactory.describeDeliveryTaskResponse(
                    responseContent, internalResponse);
            return response;
        }
    }