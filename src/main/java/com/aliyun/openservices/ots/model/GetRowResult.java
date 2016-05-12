/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public class GetRowResult extends OTSResult {
    /**
     * 单行查询返回的数据。
     */
    private Row row;
    
    /**
     * 此次操作消耗的CapacityUnit。
     */
    private ConsumedCapacity consumedCapacity;
    
    GetRowResult(OTSResult meta) {
        super(meta);
    }

    /**
     * 获取单行查询返回的数据。
     * @return
     */
    public Row getRow() {
        return row;
    }

    void setRow(Row row) {
        this.row = row;
    }
    
    /**
     * 获取此次操作消耗的CapacityUnit。
     * @return 此次操作消耗的CapacityUnit。
     */
    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        this.consumedCapacity = consumedCapacity;
    }
}
