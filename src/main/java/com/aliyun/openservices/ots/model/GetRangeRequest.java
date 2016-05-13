package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

public class GetRangeRequest {
    
    /**
     * 范围查询的条件。
     */
    private RangeRowQueryCriteria rangeRowQueryCriteria;
   
    public GetRangeRequest() {
    }
    
    public GetRangeRequest(RangeRowQueryCriteria rangeRowQueryCriteria) {
        setRangeRowQueryCriteria(rangeRowQueryCriteria);
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
        assertParameterNotNull(rangeRowQueryCriteria, "rangeRowQueryCriteria");
        this.rangeRowQueryCriteria = rangeRowQueryCriteria;
    }
}
