package com.alicloud.openservices.tablestore.core.http;

import java.io.IOException;
import java.util.List;

import com.aliyun.ots.thirdparty.org.apache.http.ContentTooLongException;
import com.aliyun.ots.thirdparty.org.apache.http.HttpEntity;
import com.aliyun.ots.thirdparty.org.apache.http.HttpResponse;
import com.aliyun.ots.thirdparty.org.apache.http.entity.ContentType;
import com.aliyun.ots.thirdparty.org.apache.http.nio.ContentDecoder;
import com.aliyun.ots.thirdparty.org.apache.http.nio.IOControl;
import com.aliyun.ots.thirdparty.org.apache.http.nio.entity.ContentBufferEntity;
import com.aliyun.ots.thirdparty.org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import com.aliyun.ots.thirdparty.org.apache.http.nio.util.HeapByteBufferAllocator;
import com.aliyun.ots.thirdparty.org.apache.http.nio.util.SimpleInputBuffer;
import com.aliyun.ots.thirdparty.org.apache.http.protocol.HttpContext;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.core.ResponseContentWithMeta;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;
import com.alicloud.openservices.tablestore.core.http.ResponseMessage;
import com.alicloud.openservices.tablestore.core.http.ExecutionContext;
import com.alicloud.openservices.tablestore.core.http.ResponseHandler;
import com.alicloud.openservices.tablestore.core.protocol.ResultParseException;
import com.alicloud.openservices.tablestore.core.protocol.ResultParser;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

public abstract class ResponseConsumer<Res>
  extends AbstractAsyncResponseConsumer<Res> {
    protected static final int BUFFER_SIZE = 4096;
    protected volatile HttpResponse httpResponse;
    protected volatile SimpleInputBuffer buf;
    protected ResultParser resultParser;
    protected ExecutionContext context;
    protected TraceLogger traceLogger;
    protected RetryStrategy retry;
    protected Res lastResult;

    public ResponseConsumer(
        ResultParser resultParser,
        TraceLogger traceLogger,
        RetryStrategy retry,
        Res lastResult)
    {
        this.resultParser = resultParser;
        this.traceLogger = traceLogger;
        this.retry = retry;
        this.lastResult = lastResult;
    }

    // Create the context and URI after creating the Consumer, so they need to be set separately.
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
            LogUtil.LOG.debug(LogUtil.TRACE_ID_WITH_COLON + traceLogger.getTraceId()
                    + LogUtil.DELIMITER + LogUtil.RESPONSE_RECEIVED);
        }
        traceLogger.addEventTime(LogUtil.RESPONSE_RECEIVED, System.currentTimeMillis());
        Res result = parseResult();
        return result;
    }

    protected ResponseContentWithMeta getResponseContentWithMeta()
            throws Exception {
        ResponseMessage response = new ResponseMessage(httpResponse);
        String traceInfo = response.getHeader(Constants.OTS_HEADER_TRACE_INFO);
        if (traceInfo != null) {
            if (LogUtil.LOG.isInfoEnabled()) {
                LogUtil.LOG.info(
                    LogUtil.TRACE_ID_WITH_COLON + traceLogger.getTraceId()
                    + LogUtil.DELIMITER + LogUtil.SERVER_TRACE_INFO_WITH_COLON
                    + traceInfo);
            }
        }
        ResponseContentWithMeta responseContent;
        try {
            List<ResponseHandler> responseHandlers =
                this.context.getResponseHandlers();
            for (ResponseHandler h : responseHandlers) {
                h.handle(response);
            }
            responseContent = (ResponseContentWithMeta) resultParser
                .getObject(response);
            closeResponseSilently(response);
            return responseContent;
        } catch (ResultParseException e) {
            closeResponseSilently(response);
            ClientException ex = new ClientException("Failed to parse response as protocol buffer message.", e);
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
        Preconditions.checkNotNull(this.buf, "Content buffer should not be null.");
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
