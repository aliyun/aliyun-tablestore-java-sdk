package com.alicloud.openservices.tablestore;

public interface TableStoreCallback<Request, Response> {
    /**
     * 当用户的异步操作成功，将调用此方法。
     *
     * @param req 用户的请求
     * @param res 用户请求的结果
     */
    public void onCompleted(final Request req, final Response res);
    /**
     * 当用户的异步操作出错，将调用此方法。
     *
     * @param req 用户的请求
     * @param ex 请求的返回结果无效、或遇到网络异常，则为ClientException；TableStore服务返回的异常，则为TableStoreException。
     */
    public void onFailed(final Request req, final Exception ex);
}
