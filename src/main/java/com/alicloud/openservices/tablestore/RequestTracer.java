package com.alicloud.openservices.tablestore;

public interface RequestTracer {

    /**
     * 标识一次Rpc调用开始，设置服务名和方法名
     * @param startRequestTraceInfo   调用开始信息类
     */
    void startRequest(StartRequestTraceInfo startRequestTraceInfo);

    //标识客户端发送RPC调用请求

    /**
     * 标识客户端发送RPC调用请求
     * @param requestSendTraceInfo   调用请求类
     */
    void requestSend(RequestSendTraceInfo requestSendTraceInfo);

    /**
     * 备份当前rpc上下文
     * @return  rpc上下文
     */
    Object getRpcContext();

    /**
     * 标识客户端收到RPC请求响应
     * @param responseReceiveTraceInfo    请求响应类
     */
    void responseReceive(ResponseReceiveTraceInfo responseReceiveTraceInfo);

    /**
     * 调用开始信息类
     */
    public static class StartRequestTraceInfo{
        private String instanceName;    // 实例名称
        private String actionName;      // 实例操作，如UpdateRow
        private String methodName;      // 请求方法，如POST

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
        private long requestSize;       //请求大小，单位byte
        private String remoteAddr;      //服务器地址
        private Object ctx;             //上下文信息

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
        private int httpStatusCode;     //http状态码
        private long responseSize;      //响应大小
        private Object ctx;             //上下文信息

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
