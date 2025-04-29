package com.alicloud.openservices.tablestore;

/**
 * {@link ClientException} occurs when the response of a request is invalid or a network exception is encountered.
 * <p>TraceId: In the logs provided by the SDK, this ID tracks the execution information of different stages of a request, and can be used to trace logs for troubleshooting.</p>
 */
public class ClientException extends RuntimeException {

    private static final long serialVersionUID = 1870835486798448798L;

    /**
     * The logs provided internally by the SDK will track the execution information of each stage of a request, and issues can be traced and troubleshooted via this ID.
     */
    private String traceId;

    /**
     * Constructs a new instance.
     */
    public ClientException() {
        super();
    }

    /**
     * Constructor.
     *
     * @param message Exception information
     */
    public ClientException(String message) {
        super(message);
    }

    /**
     * Constructor.
     *
     * @param cause the cause of the exception
     */
    public ClientException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor.
     *
     * @param message Exception message
     * @param traceId TraceId
     */
    public ClientException(String message, String traceId) {
        super(message);
        this.traceId = traceId;
    }

    /**
     * Constructor.
     *
     * @param message Exception information
     * @param cause   Reason for the exception
     */
    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor.
     *
     * @param message exception information
     * @param cause   reason for the exception
     * @param traceId TraceId
     */
    public ClientException(String message, Throwable cause, String traceId) {
        super(message, cause);
        this.traceId = traceId;
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

}
