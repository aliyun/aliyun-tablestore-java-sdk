/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

public class PutRowRequest {
    
    /**
     * PutRow操作的请求参数。
     */
    private RowPutChange rowChange;
    
    public PutRowRequest() {
        
    }
    
    public PutRowRequest(RowPutChange rowChange) {
        setRowChange(rowChange);
    }

    /**
     * 获取PutRow的请求参数。
     * @return PutRow的请求参数。
     */
    public RowPutChange getRowChange() {
        return rowChange;
    }

    /**
     * 设置PutRow的请求参数。
     * @param rowChange PutRow的请求参数。
     */
    public void setRowChange(RowPutChange rowChange) {
        assertParameterNotNull(rowChange, "rowChange");
        this.rowChange = rowChange;
    }
}
