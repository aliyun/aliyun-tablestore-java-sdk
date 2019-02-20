package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;


public class StartLocalTransactionResponse extends Response  implements Jsonizable {
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

    /*
     * 内部接口。请勿使用。
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
