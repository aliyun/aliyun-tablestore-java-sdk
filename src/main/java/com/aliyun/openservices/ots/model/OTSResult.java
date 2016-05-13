package com.aliyun.openservices.ots.model;

public class OTSResult {
    private String requestID;
    private String traceId;
    
    public OTSResult() {
        
    }
    
    public OTSResult(OTSResult meta) {
        this.requestID = meta.requestID;
    }
    
    public OTSResult(String requestID) {
        this.requestID = requestID;
    }

    public String getRequestID() {
        return requestID;
    }

    public void setRequestID(String requestID) {
        this.requestID = requestID;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }
}
