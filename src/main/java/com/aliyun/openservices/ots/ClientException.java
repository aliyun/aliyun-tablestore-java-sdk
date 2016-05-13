package com.aliyun.openservices.ots;

public class ClientException extends RuntimeException {
    
    private static final long serialVersionUID = 1870835486798448798L;
    
    private String errorCode = ClientErrorCode.UNKNOWN;
    private String traceId;
    
    /**
     * 获取异常的错误码
     * @return 异常错误码
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * 构造新实例。
     */
    public ClientException(){
        super();
    }

    /**
     * 用给定的异常信息构造新实例。
     * @param message 异常信息。
     */
    public ClientException(String message){
        super(message);
    }

    /**
     * 用表示异常原因的对象构造新实例。
     * @param cause 异常原因。
     */
    public ClientException(Throwable cause){
        super(cause);
    }
    
    public ClientException(String message, String traceId) {
        super(message);
        this.traceId = traceId;
    }
    
    /**
     * 用异常消息和表示异常原因的对象构造新实例。
     * @param message 异常信息。
     * @param cause 异常原因。
     */
    public ClientException(String message, Throwable cause){
        super(message, cause);
    }

    public ClientException(String message, Throwable cause, String traceId) {
        super(message, cause);
        this.traceId = traceId;
    }
    
    /**
     * 用异常消息和表示异常原因的对象构造新实例。
     * @param errorCode 错误码
     * @param message 异常信息。
     * @param cause 异常原因。
     */
    public ClientException(String errorCode, String message, Throwable cause){
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

}
