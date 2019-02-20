package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class PutRowRequest extends TxnRequest {
    
    /**
     * PutRow操作的请求参数。
     */
    private RowPutChange rowChange;
    
    public PutRowRequest() {
        
    }
    
    public PutRowRequest(RowPutChange rowChange) {
        setRowChange(rowChange);
    }

    /**
     * @return "PutRow"
     */
    @Override
    public String getOperationName() {
        return OperationNames.OP_PUT_ROW;
    }

    /**
     * 获取PutRow的请求参数。
     * @return PutRow的请求参数。
     */
    public RowPutChange getRowChange() {
        return rowChange;
    }

    /**
     * 设置PutRow的请求参数。
     * @param rowChange PutRow的请求参数。
     */
    public void setRowChange(RowPutChange rowChange) {
        Preconditions.checkNotNull(rowChange, "The row change for PutRow should not be null.");
        this.rowChange = rowChange;
    }
}
