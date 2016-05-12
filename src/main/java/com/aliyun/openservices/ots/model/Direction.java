/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public enum Direction {
    /**
     * 正序读。
     */
    FORWARD("FORWARD"),
    /**
     * 反序读。
     */
    BACKWARD("BACKWARD");
    
    private String name;
    
    private Direction(String name) {
        this.name = name;
    }
    
    @Override
    public String toString() {
        return name;
    }
}
