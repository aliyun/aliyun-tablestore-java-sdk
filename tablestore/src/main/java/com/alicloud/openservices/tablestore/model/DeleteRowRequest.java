package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class DeleteRowRequest extends TxnRequest {
    
    /**
     * Request parameters for the DeleteRow operation.
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
     * Get the request parameters for DeleteRow.
     * @return The request parameters for DeleteRow.
     */
    public RowDeleteChange getRowChange() {
        return rowChange;
    }

    /**
     * Set the request parameters for DeleteRow.
     * @param rowChange The request parameters for DeleteRow.
     */
    public void setRowChange(RowDeleteChange rowChange) {
        Preconditions.checkNotNull(rowChange, "The row change for DeleteRow should not be null.");
        this.rowChange = rowChange;
    }
}
