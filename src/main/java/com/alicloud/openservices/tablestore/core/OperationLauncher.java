package com.alicloud.openservices.tablestore.core;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.zip.Deflater;

import com.alicloud.openservices.tablestore.RequestTracer;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.http.*;
import com.alicloud.openservices.tablestore.core.utils.*;
import com.alicloud.openservices.tablestore.model.ExtensionRequest;
import com.google.protobuf.Message;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.auth.RequestSigner;
import com.alicloud.openservices.tablestore.model.RequestExtension;

import static com.alicloud.openservices.tablestore.core.Constants.*;

public abstract class OperationLauncher<Req, Res> {
    private static Logger logger = LoggerFactory.getLogger(OperationLauncher.class);

    private String instanceName;
    private AsyncServiceClient client;
    private CredentialsProvider crdsProvider;
    private ClientConfiguration config;
    protected Req originRequest;
    protected Res lastResult;
    protected RequestExtension requestExtension;

    public OperationLauncher(
        String instanceName,
        AsyncServiceClient client,
        CredentialsProvider crdsProvider,
        ClientConfiguration config,
        Req originRequest)
    {
        Preconditions.checkNotNull(instanceName);
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(crdsProvider);
        Preconditions.checkNotNull(config);

        this.instanceName = instanceName;
        this.client = client;
        this.crdsProvider = crdsProvider;
        this.config = config;
        this.originRequest = originRequest;

        if (originRequest instanceof ExtensionRequest) {
            requestExtension=((ExtensionRequest) originRequest).getExtension();
        }
    }

    private static ExecutionContext createContext(
        OTSUri uri,
        String instanceName,
        ServiceCredentials credentials,
        ClientConfiguration config,
        Object rpcContext)
    {
        ExecutionContext ec = new ExecutionContext();
        ec.setSigner(new RequestSigner(instanceName, credentials));

        // ResponseHandlers需要按照以下顺序.
        ec.getResponseHandlers().add(new OTSDeflateResponseHandler());
        ec.getResponseHandlers().add(new ErrorResponseHandler());
        if (config.isEnableResponseContentMD5Checking()) {
            ec.getResponseHandlers().add(new ContentMD5ResponseHandler());
        }
        if (config.isEnableResponseValidation()) {
            ec.getResponseHandlers().add(
                new OTSValidationResponseHandler(credentials, uri));
        }
        if (config.isEnableRequestTracer()) {
            ec.getResponseHandlers().add(
                    new RequestTracerResponseHandler(rpcContext, config.getRequestTracer()));
        }
        return ec;
    }

    public Req getRequestForRetry(Exception ex) {
    	return this.originRequest;
    }
    
    public abstract void fire(Req request, FutureCallback<Res> cb);

    protected void asyncInvokePost(
            OTSUri actionURI,
            Map<String, String> queryParameters,
            Message message,
            TraceLogger traceLogger,
            ResponseConsumer<Res> consumer,
            FutureCallback<Res> callback)
    {
        URI uri = buildURI(actionURI, queryParameters);
        HttpPost request = new HttpPost(uri);

        if (logger.isDebugEnabled()) {
            logger.debug("Operation: {}, PBRequestMessage: {}, TraceId: {}",
                    actionURI, message.toString(), traceLogger.getTraceId());
        }

        byte[] content = message.toByteArray();

        if (config.isEnableRequestCompression() && content != null && content.length > 0) {
            int contentLength = content.length;
            try {
                content = CompressUtil.compress(
                        new ByteArrayInputStream(content),
                        new Deflater());
                request.addHeader(OTS_HEADER_REQUEST_COMPRESS_TYPE, OTS_COMPRESS_TYPE);
                request.addHeader(
                        OTS_HEADER_REQUEST_COMPRESS_SIZE, Integer.toString(contentLength));
            } catch (IOException e) {
                throw new ClientException("RequestCompressFail: " + e.getMessage());
            }
        }
        /**
         * 接入链路追踪系统
         */
        Object rpcContext = null;
        if(config.isEnableRequestTracer()){
            if(config.getRequestTracer() == null){
                throw new ClientException("RequestTracer should not be null when enable RequestTracer");
            }
            String methodName = request.getMethod();
            String action = actionURI.getAction();
            RequestTracer.StartRequestTraceInfo startRequestTraceInfo = new RequestTracer.StartRequestTraceInfo(instanceName, action, methodName);

            config.getRequestTracer().startRequest(startRequestTraceInfo); //标识一次Rpc调用开始，设置服务名和方法名
            rpcContext = config.getRequestTracer().getRpcContext();
        }

        if (content == null) {
            content = new byte[0];
        }

        request.setEntity(new ByteArrayEntity(content));

        String contentMd5 = Base64.toBase64String(BinaryUtil.calculateMd5(content));

        // build a wrapper for HttpRequestBase to store additional information
        RequestMessage requestMessage = new RequestMessage(request);
        requestMessage.setActionUri(actionURI);
        requestMessage.setContentMd5(contentMd5);
        requestMessage.setContentLength(content.length);

        addRequiredHeaders(requestMessage, contentMd5, traceLogger.getTraceId());

        ServiceCredentials credentials = crdsProvider.getCredentials();


        ExecutionContext ctx = createContext(
                actionURI, instanceName, credentials, config, rpcContext);

        client.asyncSendRequest(requestMessage, ctx, consumer, callback, traceLogger, config.getRequestTracer(), rpcContext);
    }



    private void addRequiredHeaders(RequestMessage request, String contentMd5, String traceId) {
        request.addHeader(OTS_HEADER_SDK_TRACE_ID, traceId);
        request.addHeader(OTS_HEADER_OTS_CONTENT_MD5, contentMd5);
        request.addHeader(OTS_HEADER_API_VERSION, API_VERSION);
        request.addHeader(OTS_HEADER_INSTANCE_NAME, instanceName);
        request.addHeader(OTS_HEADER_TRACE_THRESHOLD,
                Integer.toString(config.getTimeThresholdOfServerTracer()));
        request.addHeader(OTS_HEADER_DATE, DateUtil.getCurrentIso8601Date());
        if (requestExtension != null) {
            if (requestExtension.getPriority() != null) {
                request.addHeader(OTS_HEADER_REQUEST_PRIORITY, Long.toString(requestExtension.getPriority().getValue()));
            }
            if (requestExtension.getTag() != null) {
                request.addHeader(OTS_HEADER_REQUEST_TAG, requestExtension.getTag());
            }
        }
        if (config.isEnableResponseCompression()) {
            request.addHeader(
                    OTS_HEADER_RESPONSE_COMPRESS_TYPE, OTS_COMPRESS_TYPE);
        }
    }

    
    private static URI buildURI(OTSUri actionUri, Map<String, String> queryParameter) {
        if (queryParameter == null || queryParameter.isEmpty()) {
            return actionUri.getUri();
        } else {
            try {
                URIBuilder builder = new URIBuilder(actionUri.getUri());
                for (Map.Entry<String, String> entry : queryParameter.entrySet()) {
                    builder.addParameter(entry.getKey(), entry.getValue());
                }
                return builder.build();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
    
}
