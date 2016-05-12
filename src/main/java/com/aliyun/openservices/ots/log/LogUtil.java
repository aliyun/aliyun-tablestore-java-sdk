package com.aliyun.openservices.ots.log;

import com.aliyun.openservices.ots.internal.OTSExecutionContext;
import com.aliyun.openservices.ots.internal.OTSOperation;
import com.aliyun.openservices.ots.internal.OTSTraceLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.openservices.ots.internal.OTSLoggerConstant.*;

public class LogUtil {
    public static Logger LOG = LoggerFactory.getLogger(OTSOperation.class);

    public static void logBeforeExecution(OTSExecutionContext executionContext) {
        OTSTraceLogger traceLogger = executionContext.getTraceLogger();
        int retries = executionContext.getRetries();
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

    public static void logRequestInfo(OTSTraceLogger traceLogger, String otsAction,
                                      int contentSize) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(TRACE_ID_WITH_COLON + traceLogger.getTraceId()
                    + DELIMITER + OTS_ACTION_WITH_COLON + otsAction + DELIMITER
                    + REQUEST_CONTENT_SIZE_WITH_COLON + contentSize);
        }
        traceLogger.addRequestInfo(OTS_ACTION, otsAction);
        traceLogger.addRequestInfo(REQUEST_CONTENT_SIZE, "" + contentSize);
    }

    public static void logOnCompleted(OTSExecutionContext executionContext, String requestId) {
        OTSTraceLogger traceLogger = executionContext.getTraceLogger();
        int retries = executionContext.getRetries();
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

    public static void logOnFailed(OTSExecutionContext executionContext, Exception ex, String requestId) {
        OTSTraceLogger traceLogger = executionContext.getTraceLogger();
        int retries = executionContext.getRetries();
        if (LOG.isErrorEnabled()) {
            LOG.error(TRACE_ID_WITH_COLON + traceLogger.getTraceId()
                    + DELIMITER + FAILED + DELIMITER + RETRIES_WITH_COLON
                    + retries + DELIMITER + ex);
        }
        traceLogger.addEventTime(FAILED, System.currentTimeMillis());
        if (requestId != null) {
            traceLogger.addRequestInfo(REQUEST_ID, requestId);
        }
    }
}
