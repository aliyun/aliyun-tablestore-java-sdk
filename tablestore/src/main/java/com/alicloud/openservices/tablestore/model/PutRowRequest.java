package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class PutRowRequest extends TxnRequest {
    
    /**
     * Request parameters for the PutRow operation.
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
     * Get the request parameters for PutRow.
     * @return The request parameters for PutRow.
     */
    public RowPutChange getRowChange() {
        return rowChange;
    }

    /**
     * Set the request parameters for PutRow.
     * @param rowChange The request parameters for PutRow.
     */
    public void setRowChange(RowPutChange rowChange) {
        Preconditions.checkNotNull(rowChange, "The row change for PutRow should not be null.");
        this.rowChange = rowChange;
    }
}
