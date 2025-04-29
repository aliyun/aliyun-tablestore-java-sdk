package com.alicloud.openservices.tablestore;

public interface TableStoreCallback<Request, Response> {
    /**
     * This method will be called when the user's asynchronous operation succeeds.
     *
     * @param req The user's request
     * @param res The result of the user's request
     */
    public void onCompleted(final Request req, final Response res);
    /**
     * This method will be called when the user's asynchronous operation encounters an error.
     *
     * @param req The user's request
     * @param ex If the request returns an invalid result or a network exception occurs, it will be a ClientException; if the exception is returned by the TableStore service, it will be a TableStoreException.
     */
    public void onFailed(final Request req, final Exception ex);
}
