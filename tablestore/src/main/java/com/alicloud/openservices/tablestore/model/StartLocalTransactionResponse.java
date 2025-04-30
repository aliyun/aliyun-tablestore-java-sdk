package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;


public class StartLocalTransactionResponse extends Response  implements Jsonizable {
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

    /*
     * Internal interface. Do not use.
     */
    public StartLocalTransactionResponse(Response meta) {
        super(meta);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"TransactionID\": \"");
        sb.append(this.transactionID);
        sb.append('\"');
        sb.append(newline);
        sb.append("}");
    }
}
