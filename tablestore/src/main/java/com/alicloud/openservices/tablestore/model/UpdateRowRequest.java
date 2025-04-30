package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class UpdateRowRequest extends TxnRequest {
    
    /**
     * Request parameters for the UpdateRow operation.
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
     * Get the request parameters for UpdateRow.
     * @return The request parameters for UpdateRow.
     */
    public RowUpdateChange getRowChange() {
        return rowChange;
    }

    /**
     * Set the request parameters for UpdateRow.
     * @param rowChange The request parameters for UpdateRow.
     */
    public void setRowChange(RowUpdateChange rowChange) {
        Preconditions.checkNotNull(rowChange, "The row change for UpdateRow should not be null.");
        this.rowChange = rowChange;
    }
}
