/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public enum RowExistenceExpectation {
    /**
     * 不对行是否存在做任何判断。
     */
    IGNORE("IGNORE"),
    
    /**
     * 期望该行存在。
     */
    EXPECT_EXIST("EXPECT_EXIST"),
    
    /**
     * 期望该行不存在。
     */
    EXPECT_NOT_EXIST("EXPECT_NOT_EXIST");

    private String name;
    
    private RowExistenceExpectation(String name) {
        this.name = name;
    }
    
    @Override
    public String toString() {
        return name;
    }
}
