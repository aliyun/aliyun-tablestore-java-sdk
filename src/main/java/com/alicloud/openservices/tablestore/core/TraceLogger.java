package com.alicloud.openservices.tablestore.core;

import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.alicloud.openservices.tablestore.core.utils.Pair;
import com.alicloud.openservices.tablestore.core.utils.LogUtil;

public class TraceLogger {
    private final String traceId;
    private List<Pair<String, Long>> eventTimeRecord;
    private List<Pair<String, String>> requestInfoRecord;
    private static Logger logger = LoggerFactory.getLogger(TraceLogger.class);
    private int timeThreshold;

    public TraceLogger(String traceId, int timeThreshold) {
        this.traceId = traceId;
        this.timeThreshold = timeThreshold;
        this.eventTimeRecord = new LinkedList<Pair<String, Long>>();
        this.requestInfoRecord = new LinkedList<Pair<String, String>>();
    }

    public void addRequestInfo(String key, String value) {
        this.requestInfoRecord.add(new Pair<String, String>(key, value));
    }

    public void addEventTime(String event, Long time) {
        this.eventTimeRecord.add(new Pair<String, Long>(event, time));
    }

    public void printLog() {
        if (logger.isInfoEnabled()) {
            long startTime = eventTimeRecord.get(0).getSecond();
            long totalTime = eventTimeRecord.get(eventTimeRecord.size() - 1).getSecond() - startTime;
            if (totalTime > timeThreshold) {
                StringBuilder strBuilder = new StringBuilder();
                strBuilder.append(LogUtil.TRACE_ID_WITH_COLON + traceId + LogUtil.DELIMITER);
                strBuilder.append(LogUtil.TOTAL_TIME_WITH_COLON + totalTime + LogUtil.DELIMITER);
                for (Pair<String, String> pair : requestInfoRecord) {
                    strBuilder.append(pair + LogUtil.DELIMITER);
                }
                for (Pair<String, Long> pair : eventTimeRecord) {
                    strBuilder.append(pair + LogUtil.DELIMITER);
                }
                logger.info(strBuilder.toString());
            }
        }
    }

    public String getTraceId() {
        return traceId;
    }

}
