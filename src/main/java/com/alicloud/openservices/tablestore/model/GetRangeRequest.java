package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class GetRangeRequest extends TxnRequest {
    
    /**
     * 范围查询的条件。
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
     * 获取范围查询的条件。
     * @return 范围查询的条件。
     */
    public RangeRowQueryCriteria getRangeRowQueryCriteria() {
        return rangeRowQueryCriteria;
    }

    /**
     * 设置范围查询的条件。
     * @param rangeRowQueryCriteria 范围查询的条件。
     */
    public void setRangeRowQueryCriteria(RangeRowQueryCriteria rangeRowQueryCriteria) {
        Preconditions.checkNotNull(rangeRowQueryCriteria, "The rangeRowQueryCriteria should not be null.");
        this.rangeRowQueryCriteria = rangeRowQueryCriteria;
    }
}
