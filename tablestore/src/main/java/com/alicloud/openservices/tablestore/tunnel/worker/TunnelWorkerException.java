package com.alicloud.openservices.tablestore.tunnel.worker;

public class TunnelWorkerException extends Exception {
    private String code;
    private String message;
    private String requestId;
    private String tunnelId;

    public TunnelWorkerException(String code, String message) {
        super(message);
        this.code = code;
        this.message = message;
    }

    @Override
    public String toString() {
        return String.format("RequestId: %s, Code: %s, Message: %s", requestId, code, message);
    }
}