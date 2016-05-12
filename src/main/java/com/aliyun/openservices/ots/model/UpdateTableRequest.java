/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

public class UpdateTableRequest {
    /**
     * 表的名称。
     */
    private String tableName;
    
    /**
     * 表的预留吞吐量变更。
     * 可以单独更改读CapacityUnit或者写CapacityUnit。
     */
    private ReservedThroughputChange reservedThroughputChange;
    
    public UpdateTableRequest() {
        
    }
    
    public UpdateTableRequest(String tableName) {
        setTableName(tableName);
    }
    
    public UpdateTableRequest(String tableName, ReservedThroughputChange capacityChange) {
        setTableName(tableName);
        setReservedThroughputChange(capacityChange);
    }

    /**
     * 获取表的名称。
     * @return 表的名称。
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置表的名称
     * @param tableName 表的名称
     */
    public void setTableName(String tableName) {
        assertParameterNotNull(tableName, "tableName");
        this.tableName = tableName;
    }

    /**
     * 获取表的预留吞吐量变更。
     * @return 表的预留吞吐量变更。
     */
    public ReservedThroughputChange getReservedThrougputChange() {
        return reservedThroughputChange;
    }

    /**
     * 设置表的预留吞吐量变更。
     * @param reservedThroughputChange 表的预留吞吐量更改。
     */
    public void setReservedThroughputChange(ReservedThroughputChange reservedThroughputChange) {
        assertParameterNotNull(reservedThroughputChange, "reservedThroughputChange");
        this.reservedThroughputChange = reservedThroughputChange;
    }
}
