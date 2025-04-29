package com.alicloud.openservices.tablestore.model;


public class AbortTransactionRequest implements Request {

    /**
     * The ID of the transaction.
     */
    private String transactionID;

    /**
     * Set the ID of the transaction.
     */
    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }

    /**
     * Get the ID of the transaction.
     *
     * @return The ID of the transaction
     */
    public String getTransactionID() {
        return transactionID;
    }

    /**
     * Initialize an instance of AbortTransactionRequest.
     *
     * @param transactionID The ID of the transaction.
     */
    public AbortTransactionRequest(String transactionID) {
        setTransactionID(transactionID);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_ABORT_TRANSACTION;
    }
}
