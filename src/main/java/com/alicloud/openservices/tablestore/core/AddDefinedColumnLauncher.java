package com.alicloud.openservices.tablestore.core;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.AddDefinedColumnResponseConsumer;
import org.apache.http.concurrent.FutureCallback;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.http.OTSUri;
import com.alicloud.openservices.tablestore.core.http.AsyncServiceClient;
import com.alicloud.openservices.tablestore.core.protocol.OtsInternalApi;
import com.alicloud.openservices.tablestore.core.protocol.OTSProtocolBuilder;
import com.alicloud.openservices.tablestore.core.protocol.ResultParserFactory;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import com.alicloud.openservices.tablestore.model.AddDefinedColumnRequest;
import com.alicloud.openservices.tablestore.model.AddDefinedColumnResponse;

public class AddDefinedColumnLauncher
    extends OperationLauncher<AddDefinedColumnRequest, AddDefinedColumnResponse> {
      private OTSUri uri;
      private TraceLogger tracer;
      private RetryStrategy retry;

      public AddDefinedColumnLauncher(
          OTSUri uri,
          TraceLogger tracer,
          RetryStrategy retry,
          String instanceName,
          AsyncServiceClient client,
          CredentialsProvider crdsProvider,
          ClientConfiguration config,
          AddDefinedColumnRequest originRequest)
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
      public void fire(AddDefinedColumnRequest req, FutureCallback<AddDefinedColumnResponse> cb) {
          LogUtil.logBeforeExecution(tracer, retry);

          OtsInternalApi.AddDefinedColumnResponse defaultResponse =
              OtsInternalApi.AddDefinedColumnResponse.getDefaultInstance();
          asyncInvokePost(
            uri,
            null,
            OTSProtocolBuilder.buildAddDefinedColumnRequest(req),
            tracer,
            new AddDefinedColumnResponseConsumer(
                ResultParserFactory.createFactory().createProtocolBufferResultParser(
                    defaultResponse, tracer.getTraceId()),
                tracer, retry, lastResult),
            cb);
      }
}