/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

public class UpdateRowRequest {
    
    /**
     * UpdateRow操作的请求参数。
     */
    private RowUpdateChange rowChange;
    
    public UpdateRowRequest() {
        
    }
    
    public UpdateRowRequest(RowUpdateChange rowChange) {
        setRowChange(rowChange);
    }

    /**
     * 获取UpdateRow的请求参数。
     * @return UpdateRow的请求参数。
     */
    public RowUpdateChange getRowChange() {
        return rowChange;
    }

    /**
     * 设置UpdateRow的请求参数。
     * @param rowChange
     */
    public void setRowChange(RowUpdateChange rowChange) {
        assertParameterNotNull(rowChange, "rowChange");
        this.rowChange = rowChange;
    }
}
