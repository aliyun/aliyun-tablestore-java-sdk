/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

public class GetRowRequest {
    
    /**
     * 单行查询条件。
     */
    private SingleRowQueryCriteria rowQueryCriteria;

    public GetRowRequest() {

    }
    
    /**
     * 通过单行查询条件构造GetRowRequest对象。
     * @param rowQueryCriteria 单行查询条件。
     */
    public GetRowRequest(SingleRowQueryCriteria rowQueryCriteria) {
        setRowQueryCriteria(rowQueryCriteria);
    }

    /**
     * 获取单行查询条件。
     * @return 单行查询条件。
     */
    public SingleRowQueryCriteria getRowQueryCriteria() {
        return rowQueryCriteria;
    }

    /**
     * 设置单行查询条件。
     * @param rowQueryCriteria 单行查询条件。
     */
    public void setRowQueryCriteria(SingleRowQueryCriteria rowQueryCriteria) {
        assertParameterNotNull(rowQueryCriteria, "rowQueryCriteria");
        this.rowQueryCriteria = rowQueryCriteria;
    }

}
