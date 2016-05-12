/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import com.aliyun.openservices.ots.ClientErrorCode;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.comm.ExecutionContext;
import com.aliyun.openservices.ots.comm.ResponseHandler;
import com.aliyun.openservices.ots.comm.ResponseMessage;
import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.log.LogUtil;
import com.aliyun.openservices.ots.parser.ResultParseException;
import com.aliyun.openservices.ots.parser.ResultParser;
import com.aliyun.openservices.ots.utils.ResourceManager;
import com.aliyun.openservices.ots.utils.ServiceConstants;
import org.apache.http.ContentTooLongException;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.entity.ContentBufferEntity;
import org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import org.apache.http.nio.util.HeapByteBufferAllocator;
import org.apache.http.nio.util.SimpleInputBuffer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.Asserts;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aliyun.openservices.ots.internal.OTSLoggerConstant.*;

public abstract class OTSAsyncResponseConsumer<Res> extends
        AbstractAsyncResponseConsumer<Res> {
    protected static final int BUFFER_SIZE = 4096;
    protected volatile HttpResponse httpResponse;
    protected volatile SimpleInputBuffer buf;
    protected ResultParser resultParser;
    protected ExecutionContext context;
    protected OTSTraceLogger traceLogger;
    protected static ResourceManager rm = ResourceManager
            .getInstance(ServiceConstants.RESOURCE_NAME_COMMON);

    public OTSAsyncResponseConsumer(ResultParser resultParser, OTSTraceLogger traceLogger) {
        this.resultParser = resultParser;
        this.traceLogger = traceLogger;
    }

    // 创建context和uri在创建Consumer之后，所以要单独set
    public void setContext(ExecutionContext context) {
        this.context = context;
    }

    @Override
    protected void onResponseReceived(final HttpResponse response)
            throws IOException {
        this.httpResponse = response;
    }

    protected abstract Res parseResult() throws Exception;

    @Override
    protected Res buildResult(final HttpContext context) throws Exception {
        if (LogUtil.LOG.isDebugEnabled()) {
            LogUtil.LOG.debug(TRACE_ID_WITH_COLON + traceLogger.getTraceId()
                    + DELIMITER + RESPONSE_RECEIVED);
        }
        traceLogger.addEventTime(RESPONSE_RECEIVED, System.currentTimeMillis());
        Res result = parseResult();
        return result;
    }

    protected ResponseContentWithMeta getResponseContentWithMeta()
            throws Exception {
        ResponseMessage response = new ResponseMessage(httpResponse);
        ResponseContentWithMeta responseContent;
        try {
            List<ResponseHandler> responseHandlers = this.context
                    .getResponseHandlers();
            for (ResponseHandler h : responseHandlers) {
                h.handle(response);
            }
            responseContent = (ResponseContentWithMeta) resultParser
                    .getObject(response);
            closeResponseSilently(response);
            return responseContent;
        } catch (ResultParseException e) {
            closeResponseSilently(response);
            ClientException ex = new ClientException(
                    ClientErrorCode.INVALID_RESPONSE,
                    rm.getFormattedString("FailedToParseResponse"), e);
            throw ex;
        } catch (Exception ex) {
            closeResponseSilently(response);
            throw ex;
        }
    }

    @Override
    protected void onEntityEnclosed(final HttpEntity entity,
            final ContentType contentType) throws IOException {
        long len = entity.getContentLength();
        if (len > Integer.MAX_VALUE) {
            throw new ContentTooLongException("Entity content is too long: "
                    + len);
        }
        if (len < 0) {
            len = BUFFER_SIZE;
        }
        this.buf = new SimpleInputBuffer((int) len,
                new HeapByteBufferAllocator());
        this.httpResponse.setEntity(new ContentBufferEntity(entity, this.buf));
    }

    @Override
    protected void onContentReceived(final ContentDecoder decoder,
            final IOControl ioctrl) throws IOException {
        Asserts.notNull(this.buf, "Content buffer");
        this.buf.consumeContent(decoder);
    }

    @Override
    protected void releaseResources() {
        this.httpResponse = null;
        this.buf = null;
    }

    private void closeResponseSilently(ResponseMessage response) {
        if (response != null) {
            try {
                response.close();
            } catch (IOException ioe) { /* silently close the response. */
            }
        }
    }
}