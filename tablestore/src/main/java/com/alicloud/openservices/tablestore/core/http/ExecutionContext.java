package com.alicloud.openservices.tablestore.core.http;

import java.util.LinkedList;
import java.util.List;

import com.alicloud.openservices.tablestore.core.auth.RequestSigner;

public class ExecutionContext {
    private RequestSigner signer;
    // The request handlers that handle request content in as a pipeline
    private List<RequestHandler> requestHandlers = new LinkedList<RequestHandler>();
    // The response handlers that handle response message in as a pipeline.
    private List<ResponseHandler> responseHandlers = new LinkedList<ResponseHandler>();

    public ExecutionContext() {
    }

    /**
     * @return the signer
     */
    public RequestSigner getSigner() {
        return signer;
    }

    /**
     * @param signer
     *            the signer to set
     */
    public void setSigner(RequestSigner signer) {
        this.signer = signer;
    }

    /**
     * @return the responseHandlers
     */
    public List<ResponseHandler> getResponseHandlers() {
        return responseHandlers;
    }

    public void addResponseHandler(ResponseHandler handler) {
        responseHandlers.add(handler);
    }

    public void insertResponseHandler(int position, ResponseHandler handler) {
        responseHandlers.add(position, handler);
    }

    public void removeResponseHandler(ResponseHandler handler) {
        responseHandlers.remove(handler);
    }

    /**
     * @return the requestHandlers
     */
    public List<RequestHandler> getResquestHandlers() {
        return requestHandlers;
    }

    public void addRequestHandler(RequestHandler handler) {
        requestHandlers.add(handler);
    }

    public void insertRequestHandler(int position, RequestHandler handler) {
        requestHandlers.add(position, handler);
    }

    public void removeRequestHandler(RequestHandler handler) {
        requestHandlers.remove(handler);
    }
}
