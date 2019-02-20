package com.alicloud.openservices.tablestore.core.utils;

import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.TraceLogger;
import com.alicloud.openservices.tablestore.model.RetryStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtil {
    public static final String COLON = ":";
    public static final String DELIMITER = "\t";
    public static final String OTS_ACTION = "OTSAction";
    public static final String REQUEST_CONTENT_SIZE = "RequestContentSize";
    public static final String RESPONSE_CONTENT_SIZE = "ResponseContentSize";
    public static final String REQUEST_ID = "RequestId";
    public static final String TRACE_ID_WITH_COLON = "TraceId:";
    public static final String RETRIES_WITH_COLON = "RetriedCount:";
    public static final String TOTAL_TIME_WITH_COLON = "TotalTime:";
    public static final String OTS_ACTION_WITH_COLON = "OTSAction:";
    public static final String REQUEST_CONTENT_SIZE_WITH_COLON = "RequestContentSize:";
    public static final String RESPONSE_CONTENT_SIZE_WITH_COLON = "ResponseContentSize:";
    public static final String REQUEST_ID_WITH_COLON = "RequestId:";
    public static final String SERVER_TRACE_INFO_WITH_COLON = "ServerTraceInfo:";
    public static final String FIRST_EXECUTION = "FirstExecution";
    public static final String START_RETRY = "StartRetry";
    public static final String INTO_HTTP_ASYNC_CLIENT = "IntoHttpAsyncClient";
    public static final String REQUEST_SENT = "RequestSent";
    public static final String RESPONSE_RECEIVED = "ResponseReveived";
    public static final String COMPLETED = "Completed";
    public static final String FAILED = "Failed";

    public static Logger LOG = LoggerFactory.getLogger(LogUtil.class);

    public static void logBeforeExecution(
        TraceLogger traceLogger, RetryStrategy retry)
    {
        int retries = retry.getRetries();
        if (retries == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(TRACE_ID_WITH_COLON
                        + traceLogger.getTraceId() + DELIMITER
                        + FIRST_EXECUTION);
            }
            traceLogger.addEventTime(FIRST_EXECUTION,
                    System.currentTimeMillis());
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(TRACE_ID_WITH_COLON
                        + traceLogger.getTraceId() + DELIMITER + START_RETRY
                        + retries);
            }
            traceLogger.addEventTime(START_RETRY, System.currentTimeMillis());
        }
    }

    public static void logRequestInfo(
        TraceLogger traceLogger, String otsAction,
        int contentSize)
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug(TRACE_ID_WITH_COLON + traceLogger.getTraceId()
                    + DELIMITER + OTS_ACTION_WITH_COLON + otsAction + DELIMITER
                    + REQUEST_CONTENT_SIZE_WITH_COLON + contentSize);
        }
        traceLogger.addRequestInfo(OTS_ACTION, otsAction);
        traceLogger.addRequestInfo(REQUEST_CONTENT_SIZE, "" + contentSize);
    }

    public static void logOnCompleted(
        TraceLogger traceLogger, RetryStrategy retry, String requestId)
    {
        int retries = retry.getRetries();
        if (LOG.isDebugEnabled()) {
            LOG.debug(TRACE_ID_WITH_COLON + traceLogger.getTraceId()
                    + DELIMITER + COMPLETED + DELIMITER + RETRIES_WITH_COLON
                    + retries + DELIMITER + REQUEST_ID_WITH_COLON + requestId);
        }
        traceLogger.addEventTime(COMPLETED, System.currentTimeMillis());
        if (requestId != null) {
            traceLogger.addRequestInfo(REQUEST_ID, requestId);
        }
    }

    public static void logOnFailed(
            TraceLogger traceLogger, RetryStrategy retry,
            Exception ex, String requestId, boolean mayRetry)
    {
        int retries = retry.getRetries();

        boolean isErrorLevel = false;
        if (!mayRetry) {
            if (ex instanceof TableStoreException) {
                if (((TableStoreException) ex).getHttpStatus() >= 500) {
                    isErrorLevel = true;
                }
            }
        }
        if (isErrorLevel) {
            if (LOG.isErrorEnabled()) {
                LOG.error(TRACE_ID_WITH_COLON + traceLogger.getTraceId()
                        + DELIMITER + FAILED + DELIMITER + RETRIES_WITH_COLON
                        + retries + DELIMITER + ex);
            }
        } else if (LOG.isWarnEnabled()) {
            LOG.warn(TRACE_ID_WITH_COLON + traceLogger.getTraceId()
                    + DELIMITER + FAILED + DELIMITER + RETRIES_WITH_COLON
                    + retries + DELIMITER + ex);
        }
        traceLogger.addEventTime(FAILED, System.currentTimeMillis());
        if (requestId != null) {
            traceLogger.addRequestInfo(REQUEST_ID, requestId);
        }
    }

    public static void logOnFailed(
        TraceLogger traceLogger, RetryStrategy retry,
        Exception ex, String requestId)
    {
        logOnFailed(traceLogger, retry, ex, requestId, true);
    }

}
