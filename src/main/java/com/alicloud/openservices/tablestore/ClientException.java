package com.alicloud.openservices.tablestore;

/**
 * {@link ClientException} 发生在请求的返回结果无效或遇到网络异常。
 * <p>TraceId: 在SDK内部提供的日志中会追踪某个请求的各个阶段的执行信息，可以通过这个ID来追踪日志排查问题。</p>
 */
public class ClientException extends RuntimeException {

    private static final long serialVersionUID = 1870835486798448798L;

    /**
     * 在SDK内部提供的日志中会追踪某个请求的各个阶段的执行信息，可以通过这个ID来追踪日志排查问题。
     */
    private String traceId;

    /**
     * 构造新实例。
     */
    public ClientException() {
        super();
    }

    /**
     * 构造函数。
     *
     * @param message 异常信息
     */
    public ClientException(String message) {
        super(message);
    }

    /**
     * 构造函数。
     *
     * @param cause 异常原因
     */
    public ClientException(Throwable cause) {
        super(cause);
    }

    /**
     * 构造函数。
     *
     * @param message 异常信息
     * @param traceId TraceId
     */
    public ClientException(String message, String traceId) {
        super(message);
        this.traceId = traceId;
    }

    /**
     * 构造函数。
     *
     * @param message 异常信息
     * @param cause   异常原因
     */
    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 构造函数。
     *
     * @param message 异常信息
     * @param cause   异常原因
     * @param traceId TraceId
     */
    public ClientException(String message, Throwable cause, String traceId) {
        super(message, cause);
        this.traceId = traceId;
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

}
