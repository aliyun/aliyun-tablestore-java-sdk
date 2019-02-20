package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.core.utils.OptionalValue;

public abstract class TxnRequest implements Request{
    /**
     * 事务ID。
     */
    private OptionalValue<String> transactionId = new OptionalValue<String>("TransactionId");

    /**
     * 设置本次事务的ID。
     *
     * @param transactionId 本次事务的ID
     */
    public void setTransactionId(String transactionId) {
        this.transactionId.setValue(transactionId);
    }

    /**
     * 获取TransactionId的设置的值。
     *
     * @return TransactionId
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public String getTransactionId() {
        if (!this.transactionId.isValueSet()) {
            throw new IllegalStateException("The value of TransactionId is not set.");
        }
        return this.transactionId.getValue();
    }

    /**
     * 查询是否设置了TransactionId。
     *
     * @return 若设置了TransactionId，则返回true，否则返回false。
     */
    public boolean hasSetTransactionId() {
        return this.transactionId.isValueSet();
    }
}
