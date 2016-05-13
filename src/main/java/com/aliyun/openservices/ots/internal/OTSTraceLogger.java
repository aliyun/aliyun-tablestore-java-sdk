package com.aliyun.openservices.ots.internal;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aliyun.openservices.ots.internal.OTSLoggerConstant.*;

public class OTSTraceLogger {
    private String traceId;
    private List<Pair<String, Long>> eventTimeRecord;
    private List<Pair<String, String>> requestInfoRecord;
    private static Logger logger = LoggerFactory.getLogger(OTSTraceLogger.class);
    private int timeThreshold;

    private class Pair<K, V> {
        private K key;
        private V value;

        public Pair(K key, V value) {
            this.setKey(key);
            this.setValue(value);
        }

        public K getKey() {
            return key;
        }

        public void setKey(K key) {
            this.key = key;
        }

        public V getValue() {
            return value;
        }

        public void setValue(V value) {
            this.value = value;
        }

        public String toString() {
            return getKey() + ":" + getValue();
        }
    }

    public OTSTraceLogger(String traceId, int timeThreshold) {
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
        if (logger.isWarnEnabled()) {
            long startTime = eventTimeRecord.get(0).getValue();
            long totalTime = eventTimeRecord.get(eventTimeRecord.size() - 1)
                    .getValue() - startTime;
            if (totalTime > timeThreshold) {
                StringBuilder strBuilder = new StringBuilder();
                strBuilder.append(TRACE_ID_WITH_COLON + traceId + DELIMITER);
                strBuilder
                        .append(TOTAL_TIME_WITH_COLON + totalTime + DELIMITER);
                for (Pair<String, String> pair : requestInfoRecord) {
                    strBuilder.append(pair + DELIMITER);
                }
                for (Pair<String, Long> pair : eventTimeRecord) {
                    strBuilder.append(pair + DELIMITER);
                }
                logger.warn(strBuilder.toString());
            }
        }
    }

    public String getTraceId() {
        return traceId;
    }

}