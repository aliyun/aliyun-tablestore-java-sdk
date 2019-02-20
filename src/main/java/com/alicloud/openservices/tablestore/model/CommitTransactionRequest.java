package com.alicloud.openservices.tablestore.model;


public class CommitTransactionRequest implements Request {

    /**
     * 事务的ID。
     */
    private String transactionID;

    /**
     *  设置事务的ID。
     */
    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }

    /**
     *  获取事务的ID。
     *
     * @return 事务的ID
     */
    public String getTransactionID() {
        return transactionID;
    }

    /**
     * 初始化CommitTransactionRequest实例。
     *
     * @param transactionID 事务的ID。
     */
    public CommitTransactionRequest(String transactionID) {
        setTransactionID(transactionID);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_COMMIT_TRANSACTION;
    }
}
