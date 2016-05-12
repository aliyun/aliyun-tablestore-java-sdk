/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

public class DeleteRowRequest {
    
    /**
     * DeleteRow操作的请求参数。
     */
    private RowDeleteChange rowChange;
    
    public DeleteRowRequest() {
        
    }
    
    public DeleteRowRequest(RowDeleteChange rowChange) {
        setRowChange(rowChange);
    }

    /**
     * 获取DeleteRow操作的请求参数。
     * @return
     */
    public RowDeleteChange getRowChange() {
        return rowChange;
    }

    /**
     * 设置DeleteRow操作的请求参数。
     * @param rowChange DeleteRow操作的请求参数。
     */
    public void setRowChange(RowDeleteChange rowChange) {
        assertParameterNotNull(rowChange, "rowChange");
        this.rowChange = rowChange;
    }
}
