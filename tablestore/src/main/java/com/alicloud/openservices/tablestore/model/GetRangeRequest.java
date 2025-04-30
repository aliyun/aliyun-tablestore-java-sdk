package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class GetRangeRequest extends TxnRequest {
    
    /**
     * The condition for range queries.
     */
    private RangeRowQueryCriteria rangeRowQueryCriteria;
   
    public GetRangeRequest() {
    }
    
    public GetRangeRequest(RangeRowQueryCriteria rangeRowQueryCriteria) {
        setRangeRowQueryCriteria(rangeRowQueryCriteria);
    }

    public String getOperationName() {
        return OperationNames.OP_GET_RANGE;
    }
    
    /**
     * Get the conditions for a range query.
     * @return The conditions for a range query.
     */
    public RangeRowQueryCriteria getRangeRowQueryCriteria() {
        return rangeRowQueryCriteria;
    }

    /**
     * Set the criteria for the range query.
     * @param rangeRowQueryCriteria The criteria for the range query.
     */
    public void setRangeRowQueryCriteria(RangeRowQueryCriteria rangeRowQueryCriteria) {
        Preconditions.checkNotNull(rangeRowQueryCriteria, "The rangeRowQueryCriteria should not be null.");
        this.rangeRowQueryCriteria = rangeRowQueryCriteria;
    }
}
