/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import java.util.List;

public class GetRangeResult extends OTSResult {
    /**
     * 此次操作消耗的CapacityUnit。
     */
    private ConsumedCapacity consumedCapacity;
    
    /**
     * 范围查询返回的所有行。
     */
    private List<Row> rows;
    
    /**
     * 下一次查询的范围的起始边界。
     */
    private RowPrimaryKey nextStartPrimaryKey;

    public GetRangeResult(OTSResult meta) {
        super(meta);
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

    /**
     * 获取本次查询返回的所有行。
     * @return
     */
    public List<Row> getRows() {
        return rows;
    }

    void setRows(List<Row> rows) {
        this.rows = rows;
    }

    /**
     * 获取下一次查询的范围的起始边界。
     * 若为null，则代表本次查询已经返回所有数据，无需再次查询。
     * @return
     */
    public RowPrimaryKey getNextStartPrimaryKey() {
        return nextStartPrimaryKey;
    }

    void setNextStartPrimaryKey(RowPrimaryKey nextStartPrimaryKey) {
        this.nextStartPrimaryKey = nextStartPrimaryKey;
    }
}
