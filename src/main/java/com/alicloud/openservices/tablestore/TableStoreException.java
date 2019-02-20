package com.alicloud.openservices.tablestore;

/**
 * {@link TableStoreException} 表示TableStore返回的异常。
 * <p>RequestId: TableStore为每个请求分配的唯一ID，若需要TableStore开发协助调查慢请求或者失败请求等问题，请提供该ID。</p>
 * <p>TraceId: 在SDK内部提供的日志中会追踪某个请求的各个阶段的执行信息，可以通过这个ID来追踪日志排查问题。</p>
 * <p>HttpStatus: 本次请求的Http返回码</p>
 */

public class TableStoreException extends RuntimeException {

    private static final long serialVersionUID = 781283289032342L;

    /**
     * TableStore返回的错误码。
     */
    private String errorCode;

    /**
     * TableStore为每个请求分配的唯一ID，若需要TableStore开发协助调查慢请求或者失败请求等问题，请提供该ID。
     */
    private String requestId;

    /**
     * 在SDK内部提供的日志中会追踪某个请求的各个阶段的执行信息，可以通过这个ID来追踪日志排查问题。
     */
    private String traceId;

    /**
     * 本次操作的Http返回状态码。
     */
    private int httpStatus;

    /**
     * 构造函数。
     *
     * @param message    错误消息
     * @param cause      错误原因
     * @param errorCode  错误代码
     * @param requestId  RequestId
     * @param httpStatus Http状态码
     */
    public TableStoreException(String message, Throwable cause,
                        String errorCode,
                        String requestId, int httpStatus) {
        super(message, cause);
        this.errorCode = errorCode;
        this.requestId = requestId;
        this.httpStatus = httpStatus;
    }

    /**
     * 构造函数
     * @param message 错误信息
     * @param errorCode 错误代码
     */
    public TableStoreException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * 返回错误代码的字符串表示。
     *
     * @return 错误代码的字符串表示。
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * 返回Request标识。
     *
     * @return Request标识。
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * 返回TraceId。
     *
     * @return TraceId。
     */
    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }


    /**
     * 返回Http状态码
     *
     * @return Http状态码
     */
    public int getHttpStatus() {
        return httpStatus;
    }

    @Override
    public String toString() {
        return "[ErrorCode]:" + errorCode + ", " +
                "[Message]:" + getMessage() + ", [RequestId]:" + requestId +
                ", [TraceId]:" + traceId + ", [HttpStatus:]" + httpStatus;
    }

}
