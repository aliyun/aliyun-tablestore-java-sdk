/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSServiceConfiguration;
import com.aliyun.openservices.ots.auth.ServiceCredentials;
import com.aliyun.openservices.ots.comm.*;
import com.aliyun.openservices.ots.utils.BinaryUtil;
import com.aliyun.openservices.ots.utils.DateUtil;
import com.aliyun.openservices.ots.utils.HttpHeaders;
import com.google.protobuf.Message;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ByteArrayEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.zip.Deflater;

import static com.aliyun.openservices.ots.internal.OTSConsts.API_VERSION;
import static com.aliyun.openservices.ots.internal.OTSHTTPConstant.*;
import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;
import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;

/**
 * 表示对OTS进行操作的类。
 *
 */
public abstract class OTSOperation {
    protected Logger logger = LoggerFactory.getLogger(OTSOperation.class);

    private String instanceName;
    private ServiceClient client;
    private ServiceCredentials credentials;
    private OTSServiceConfiguration serviceConfig;
    private Map<String, String> extraHeaders;

    public OTSOperation(String instanceName,
            ServiceClient client, ServiceCredentials credentials,
            OTSServiceConfiguration serviceConfig) {
        assertParameterNotNull(instanceName, "instanceName");
        assertParameterNotNull(client, "client");
        assertParameterNotNull(credentials, "credentials");
        assertParameterNotNull(serviceConfig, "serviceConfig");
        this.instanceName = instanceName;
        this.client = client;
        this.credentials = credentials;
        this.serviceConfig = serviceConfig;
    }

    private URI buildURI(OTSUri actionUri, Map<String, String> queryParameter) {
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

    protected <Res> void asyncInvokePost(OTSUri actionURI,
                                         Map<String, String> queryParameters, Message message,
                                         OTSTraceLogger traceLogger,
                                         OTSAsyncResponseConsumer<Res> consumer,
                                         FutureCallback<Res> callback) throws ClientException {
        URI uri = buildURI(actionURI, queryParameters);
        HttpPost request = new HttpPost(uri);

        if (logger.isDebugEnabled()) {
            logger.debug("Operation: {}, PBRequestMessage: {}, TraceId: {}",
                    actionURI.getAction(), message.toString(), traceLogger.getTraceId());
        }

        byte[] content = message.toByteArray();
        if (content == null) {
            content = new byte[0];
        }

        byte[] dataToSend = null;
        if (this.serviceConfig.isEnableRequestCompression()) {
            try {
                dataToSend = OTSCompressUtil.compress(new ByteArrayInputStream(
                        content), new Deflater());
            } catch (IOException e) {
                throw new ClientException(
                        OTS_RESOURCE_MANAGER.getFormattedString(
                                "RequestCompressFail", e.getMessage()));
            }
            request.addHeader(OTS_HEADER_REQUEST_COMPRESS_TYPE,
                    OTS_COMPRESS_TYPE);
            request.addHeader(OTS_HEADER_REQUEST_COMPRESS_SIZE,
                    Integer.toString(content.length));
        } else {
            dataToSend = content;
        }

        request.setEntity(new ByteArrayEntity(dataToSend));

        String contentMd5 = BinaryUtil.toBase64String(BinaryUtil.calculateMd5(dataToSend));

        // build a wrapper for HttpRequestBase to store additional information
        RequestMessage requestMessage = new RequestMessage(request);
        requestMessage.setQueryParameters(queryParameters);
        requestMessage.setActionUri(actionURI);
        requestMessage.setContentLength(content.length);

        addRequiredHeaders(requestMessage, contentMd5, traceLogger.getTraceId());

        client.asyncSendRequest(requestMessage, createContext(actionURI.getAction()), consumer, callback, traceLogger);
    }

    private ExecutionContext createContext(String otsAction) {
        ExecutionContext ec = new ExecutionContext();
        ec.setCharset(OTSConsts.DEFAULT_ENCODING);
        ec.setSigner(new OTSRequestSigner(otsAction, credentials));

        // OTSExceptionResponseHandler必须在OTSValidationResponseHandler之前,
        // 因为如果返回结果引发异常时将不需要再进行验证。
        if (this.serviceConfig.isEnableResponseContentMD5Checking()) {
            ec.getResponseHandlers().add(new OTSContentMD5ResponseHandler());
        }
        ec.getResponseHandlers().add(new OTSDeflateResponseHandler());
        ec.getResponseHandlers().add(new OTSErrorResponseHandler());
        if (this.serviceConfig.isEnableResponseValidation()) {
            ec.getResponseHandlers().add(
                    new OTSValidationResponseHandler(credentials, otsAction));
        }
        return ec;
    }

    private void addRequiredHeaders(RequestMessage request, String contentMd5, String traceId) {
        request.addHeader(OTS_HEADER_OTS_CONTENT_MD5, contentMd5);
        request.addHeader(OTS_HEADER_API_VERSION, API_VERSION);
        request.addHeader(OTS_HEADER_INSTANCE_NAME, instanceName);
        request.addHeader(OTS_HEADER_DATE, DateUtil.getCurrentRfc822Date());
        request.addHeader(OTS_HEADER_ACCESS_KEY_ID, credentials.getAccessKeyId());
        if (credentials.getStsToken() != null && !credentials.getStsToken().isEmpty()) {
            request.addHeader(OTS_HEADER_STS_TOKEN, credentials.getStsToken());
        }
        if (this.serviceConfig.isEnableResponseCompression()) {
            request.addHeader(OTS_HEADER_RESPONSE_COMPRESS_TYPE,
                    OTS_COMPRESS_TYPE);
        }
        request.addHeader(OTS_HEADER_SDK_TRACE_ID, traceId);
        // Set content type and encoding
        request.addHeader(HttpHeaders.CONTENT_TYPE,
                "application/x-www-form-urlencoded; " + "charset="
                        + OTSConsts.DEFAULT_ENCODING);

        if (extraHeaders != null) {
            for (Map.Entry<String, String> entry : extraHeaders.entrySet()) {
                request.addHeader(entry.getKey(), entry.getValue());
            }
        }
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.extraHeaders = extraHeaders;
    }
}