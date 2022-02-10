package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.http.DeleteDeliveryTaskResponseConsumer;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.OtsDelivery;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.delivery.DeleteDeliveryTaskRequest;
import com.alicloud.openservices.tablestore.model.delivery.DeleteDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import org.apache.http.concurrent.FutureCallback;

public class DeleteDeliveryTaskLauncher
        extends OperationLauncher<DeleteDeliveryTaskRequest, DeleteDeliveryTaskResponse> {
        private OTSUri uri;
        private TraceLogger tracer;
        private RetryStrategy retry;

    public DeleteDeliveryTaskLauncher(
                OTSUri uri,
                TraceLogger tracer,
                RetryStrategy retry,
                String instanceName,
                AsyncServiceClient client,
                CredentialsProvider crdsProvider,
                ClientConfiguration config,
                DeleteDeliveryTaskRequest originRequest)
        {
            super(instanceName, client, crdsProvider, config, originRequest);

            Preconditions.checkNotNull(uri);
            Preconditions.checkNotNull(tracer);
            Preconditions.checkNotNull(retry);

            this.uri = uri;
            this.tracer = tracer;
            this.retry = retry;
        }

        @Override
       public void fire(DeleteDeliveryTaskRequest req, FutureCallback<DeleteDeliveryTaskResponse> cb) {
            LogUtil.logBeforeExecution(tracer, retry);

            OtsDelivery.DeleteDeliveryTaskResponse defaultResponse =
                    OtsDelivery.DeleteDeliveryTaskResponse.getDefaultInstance();
            asyncInvokePost(
                    uri,
                    null,
                    OTSProtocolBuilder.buildDeleteDeliveryTaskRequest(req),
                    tracer,
                    new DeleteDeliveryTaskResponseConsumer(
                            ResultParserFactory.createFactory().createProtocolBufferResultParser(
                                    defaultResponse, tracer.getTraceId()),
                            tracer, retry, lastResult),
                    cb);
        }
    }