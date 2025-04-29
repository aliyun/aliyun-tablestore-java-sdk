package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * The base class for all TableStore return results, containing meta-attribute information of the return result, such as RequestId, TraceId, etc.
 * <p>RequestId: The unique ID assigned by TableStore to each request. If you need assistance from TableStore developers to investigate issues with slow requests or failed requests, please provide this ID.</p>
 * <p>TraceId: In the logs provided internally by the SDK, this ID tracks the execution information of various stages of a particular request, and can be used to trace logs for troubleshooting problems.</p>
 */
public class Response {
    private String requestId;
    private String traceId;

    /**
     * Internal constructor, please do not use.
     */
    public Response() {
    }

    /**
     * Internal constructor, please do not use.
     */
    public Response(Response meta) {
        Preconditions.checkNotNull(meta, "meta must not be null.");
        this.requestId = meta.requestId;
        this.traceId = meta.traceId;
    }

    /**
     * Internal constructor, please do not use.
     * @param requestId
     */
    public Response(String requestId) {
        Preconditions.checkNotNull(requestId, "requestId must not be null.");
        this.requestId = requestId;
    }

    /**
     * Get the RequestId of this request.
     * @return RequestId
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * Set RequestId.
     * @param requestId
     */
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    /**
     * Get the TraceId of this request.
     * @return TraceId
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * Set the TraceId.
     * @param traceId The TraceId.
     */
    public void setTraceId(String traceId) {
        Preconditions.checkNotNull(traceId, "traceId must not be null.");
        this.traceId = traceId;
    }
}
