package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * 所有TableStore返回结果的基类，包含返回结果的元属性信息，例如RequestId、TraceId等。
 * <p>RequestId: TableStore为每个请求分配的唯一ID，若需要TableStore开发协助调查慢请求或者失败请求等问题，请提供该ID。</p>
 * <p>TraceId: 在SDK内部提供的日志中会追踪某个请求的各个阶段的执行信息，可以通过这个ID来追踪日志排查问题。</p>
 */
public class Response {
    private String requestId;
    private String traceId;

    /**
     * 内部用构造器，请不要使用。
     */
    public Response() {
    }

    /**
     * 内部用构造器，请不要使用。
     */
    public Response(Response meta) {
        Preconditions.checkNotNull(meta, "meta must not be null.");
        this.requestId = meta.requestId;
        this.traceId = meta.traceId;
    }

    /**
     * 内部用构造器，请不要使用。
     * @param requestId
     */
    public Response(String requestId) {
        Preconditions.checkNotNull(requestId, "requestId must not be null.");
        this.requestId = requestId;
    }

    /**
     * 获取该请求的RequestId。
     * @return RequestId
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * 设置RequestId。
     * @param requestId
     */
    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    /**
     * 获取该请求的TraceId。
     * @return TraceId
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * 设置TraceId。
     * @param traceId TraceId。
     */
    public void setTraceId(String traceId) {
        Preconditions.checkNotNull(traceId, "traceId must not be null.");
        this.traceId = traceId;
    }
}
