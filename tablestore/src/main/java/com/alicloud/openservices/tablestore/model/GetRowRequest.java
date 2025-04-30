package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * Query parameters for GetRow.
 */
public class GetRowRequest extends TxnRequest {

    /**
     * Single row query condition.
     */
    private SingleRowQueryCriteria rowQueryCriteria;

    public GetRowRequest() {

    }

    /**
     * Constructs a GetRowRequest object through a single-row query condition.
     *
     * @param rowQueryCriteria The single-row query condition.
     */
    public GetRowRequest(SingleRowQueryCriteria rowQueryCriteria) {
        setRowQueryCriteria(rowQueryCriteria);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_ROW;
    }

    /**
     * Get the query condition for a single row.
     *
     * @return The query condition for a single row.
     */
    public SingleRowQueryCriteria getRowQueryCriteria() {
        return rowQueryCriteria;
    }

    /**
     * Set the single row query condition.
     *
     * @param rowQueryCriteria The single row query condition.
     */
    public void setRowQueryCriteria(SingleRowQueryCriteria rowQueryCriteria) {
        Preconditions.checkNotNull(rowQueryCriteria, "The row query criteria should not be null.");
        this.rowQueryCriteria = rowQueryCriteria;
    }
}
