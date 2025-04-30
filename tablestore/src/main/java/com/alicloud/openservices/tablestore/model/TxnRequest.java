package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.core.utils.OptionalValue;

public abstract class TxnRequest extends ExtensionRequest {
    /**
     * Transaction ID.
     */
    private OptionalValue<String> transactionId = new OptionalValue<String>("TransactionId");

    /**
     * Set the ID for this transaction.
     *
     * @param transactionId The ID for this transaction
     */
    public void setTransactionId(String transactionId) {
        this.transactionId.setValue(transactionId);
    }

    /**
     * Get the set value of TransactionId.
     *
     * @return TransactionId
     * @throws java.lang.IllegalStateException if this parameter is not configured
     */
    public String getTransactionId() {
        if (!this.transactionId.isValueSet()) {
            throw new IllegalStateException("The value of TransactionId is not set.");
        }
        return this.transactionId.getValue();
    }

    /**
     * Query whether the TransactionId is set.
     *
     * @return If the TransactionId is set, return true; otherwise, return false.
     */
    public boolean hasSetTransactionId() {
        return this.transactionId.isValueSet();
    }
}
