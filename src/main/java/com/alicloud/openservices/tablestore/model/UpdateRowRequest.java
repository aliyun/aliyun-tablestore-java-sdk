package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class UpdateRowRequest extends TxnRequest {
    
    /**
     * UpdateRow操作的请求参数。
     */
    private RowUpdateChange rowChange;
    
    public UpdateRowRequest() {
        
    }
    
    public UpdateRowRequest(RowUpdateChange rowChange) {
        setRowChange(rowChange);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_ROW;
    }

    /**
     * 获取UpdateRow的请求参数。
     * @return UpdateRow的请求参数。
     */
    public RowUpdateChange getRowChange() {
        return rowChange;
    }

    /**
     * 设置UpdateRow的请求参数。
     * @param rowChange UpdateRow的请求参数。
     */
    public void setRowChange(RowUpdateChange rowChange) {
        Preconditions.checkNotNull(rowChange, "The row change for UpdateRow should not be null.");
        this.rowChange = rowChange;
    }
}
