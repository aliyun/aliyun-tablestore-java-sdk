package com.alicloud.openservices.tablestore;

/**
 * {@link TableStoreException} represents the exception returned by TableStore.
 * <p>RequestId: A unique ID assigned by TableStore to each request. If you need assistance from TableStore developers to investigate issues with slow requests or failed requests, please provide this ID.</p>
 * <p>TraceId: In the logs provided internally by the SDK, this ID tracks the execution information of various stages of a particular request, which can be used to trace logs for troubleshooting problems.</p>
 * <p>HttpStatus: The HTTP status code returned for this request.</p>
 */

public class TableStoreException extends RuntimeException {

    private static final long serialVersionUID = 781283289032342L;

    /**
     * Error codes returned by TableStore.
     */
    private String errorCode;

    /**
     * The unique ID assigned by TableStore to each request. If you need TableStore development team to help investigate issues related to slow requests or failed requests, please provide this ID.
     */
    private String requestId;

    /**
     * The execution information of each stage of a request will be tracked in the logs provided by the SDK internally, and the log can be traced and problems can be troubleshooted through this ID.
     */
    private String traceId;

    /**
     * The HTTP return status code for this operation.
     */
    private int httpStatus;

    /**
     * Constructor.
     *
     * @param message    Error message
     * @param cause      Cause of the error
     * @param errorCode  Error code
     * @param requestId  RequestId
     * @param httpStatus HTTP status code
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
     * Constructor
     * @param message Error message
     * @param errorCode Error code
     */
    public TableStoreException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    /**
     * Returns the string representation of the error code.
     *
     * @return The string representation of the error code.
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Returns the Request ID.
     *
     * @return Request ID.
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * Returns the TraceId.
     *
     * @return TraceId.
     */
    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }


    /**
     * Returns the HTTP status code
     *
     * @return HTTP status code
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
