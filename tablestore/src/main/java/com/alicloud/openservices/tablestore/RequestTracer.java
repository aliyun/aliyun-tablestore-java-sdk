package com.alicloud.openservices.tablestore;

public interface RequestTracer {

    /**
     * Identifies the start of an Rpc call, and sets the service name and method name.
     * @param startRequestTraceInfo   The class of invocation start information
     */
    void startRequest(StartRequestTraceInfo startRequestTraceInfo);

    // Indicates that the client sends an RPC call request.

    /**
     * Identifies the client sending RPC call requests
     * @param requestSendTraceInfo   Invocation request class
     */
    void requestSend(RequestSendTraceInfo requestSendTraceInfo);

    /**
     * Backup the current RPC context
     * @return RPC context
     */
    Object getRpcContext();

    /**
     * Indicates that the client has received an RPC request response
     * @param responseReceiveTraceInfo    Request response class
     */
    void responseReceive(ResponseReceiveTraceInfo responseReceiveTraceInfo);

    /**
     * Invocation start information class
     */
    public static class StartRequestTraceInfo{
        private String instanceName;    // Instance name
        private String actionName;      // Instance operations, such as UpdateRow
        private String methodName;      // Request method, such as POST

        public StartRequestTraceInfo(String instanceName, String actionName, String methodName) {
            this.instanceName = instanceName;
            this.actionName = actionName;
            this.methodName = methodName;
        }

        public String getInstanceName() {
            return instanceName;
        }

        public void setInstanceName(String instanceName) {
            this.instanceName = instanceName;
        }

        public String getActionName() {
            return actionName;
        }

        public void setActionName(String actionName) {
            this.actionName = actionName;
        }

        public String getMethodName() {
            return methodName;
        }

        public void setMethodName(String methodName) {
            this.methodName = methodName;
        }
    }

    public static class RequestSendTraceInfo{
        private long requestSize;       // Request size, in bytes
        private String remoteAddr;      // Server address
        private Object ctx;             // Context information

        public RequestSendTraceInfo(long requestSize, String remoteAddr, Object ctx) {
            this.requestSize = requestSize;
            this.remoteAddr = remoteAddr;
            this.ctx = ctx;
        }

        public long getRequestSize() {
            return requestSize;
        }

        public void setRequestSize(long requestSize) {
            this.requestSize = requestSize;
        }

        public String getRemoteAddr() {
            return remoteAddr;
        }

        public void setRemoteAddr(String remoteAddr) {
            this.remoteAddr = remoteAddr;
        }

        public Object getCtx() {
            return ctx;
        }

        public void setCtx(Object ctx) {
            this.ctx = ctx;
        }
    }


    public static class ResponseReceiveTraceInfo{
        private int httpStatusCode;     // HTTP status code
        private long responseSize;      // Response size
        private Object ctx;             // Context information

        public ResponseReceiveTraceInfo(int httpStatusCode, long responseSize, Object ctx) {
            this.httpStatusCode = httpStatusCode;
            this.responseSize = responseSize;
            this.ctx = ctx;
        }

        public int getHttpStatusCode() {
            return httpStatusCode;
        }

        public void setHttpStatusCode(int httpStatusCode) {
            this.httpStatusCode = httpStatusCode;
        }

        public long getResponseSize() {
            return responseSize;
        }

        public void setResponseSize(long responseSize) {
            this.responseSize = responseSize;
        }

        public Object getCtx() {
            return ctx;
        }

        public void setCtx(Object ctx) {
            this.ctx = ctx;
        }
    }

}
