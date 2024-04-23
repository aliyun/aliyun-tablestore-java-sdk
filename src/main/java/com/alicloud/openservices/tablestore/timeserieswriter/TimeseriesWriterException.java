package com.alicloud.openservices.tablestore.timeserieswriter;

public class TimeseriesWriterException extends RuntimeException {

    private static final long serialVersionUID = -8306502318732297905L;
    private String errorCode;

    public TimeseriesWriterException(String message, Throwable cause, String errorCode) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public TimeseriesWriterException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String toString() {
        return "TimeseriesWriterException{" +
                "errorCode='" + errorCode + '\'' +
                '}';
    }
}
