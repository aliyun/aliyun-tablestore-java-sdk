package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class DeleteRowRequest extends TxnRequest {
    
    /**
     * DeleteRow操作的请求参数。
     */
    private RowDeleteChange rowChange;
    
    public DeleteRowRequest() {
        
    }
    
    public DeleteRowRequest(RowDeleteChange rowChange) {
        setRowChange(rowChange);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DELETE_ROW;
    }

    /**
     * 获取DeleteRow的请求参数。
     * @return DeleteRow的请求参数。
     */
    public RowDeleteChange getRowChange() {
        return rowChange;
    }

    /**
     * 设置DeleteRow的请求参数。
     * @param rowChange DeleteRow的请求参数。
     */
    public void setRowChange(RowDeleteChange rowChange) {
        Preconditions.checkNotNull(rowChange, "The row change for DeleteRow should not be null.");
        this.rowChange = rowChange;
    }
}
