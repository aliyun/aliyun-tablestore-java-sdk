package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.model.Response;

import java.util.ArrayList;
import java.util.List;

public class PartialResultFailedException extends TableStoreException {

    private Response response;
    private List<TableStoreException> errors = new ArrayList<TableStoreException>();

    /**
     * Constructor.
     *
     * @param cause      Reason for the error
     * @param requestId  RequestId
     * @param response   Result returned by the request
     */
    public PartialResultFailedException(Throwable cause, String requestId, Response response) {
        super("Partial result failed in batch operations.", cause, "", requestId, 0);
        this.response = response;
    }

    public void addError(TableStoreException ex) {
        errors.add(ex);
    }

    public List<TableStoreException> getErrors() {
        return this.errors;
    }

    public Response getResult() {
        return this.response;
    }

}
