/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public class UpdateTableResult extends OTSResult {
    /**
     * 表当前的预留吞吐量的更改信息。
     */
    private ReservedThroughputDetails reservedThroughputDetails;

    UpdateTableResult() {
        
    }
    
    UpdateTableResult(OTSResult meta) {
        super(meta);
    }

    /**
     * 获取表当前的预留吞吐量的更改信息。
     * @return 表当前的预留吞吐量的更改信息。
     */
    public ReservedThroughputDetails getReservedThroughputDetails() {
        return reservedThroughputDetails;
    }

    void setReservedThroughputDetails(ReservedThroughputDetails reservedThroughputDetails) {
        this.reservedThroughputDetails = reservedThroughputDetails;
    }
}
