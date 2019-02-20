/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.alicloud.openservices.tablestore.model;

public enum RowExistenceExpectation {
    /**
     * 不对行是否存在做任何判断。
     */
    IGNORE,
    
    /**
     * 期望该行存在。
     */
    EXPECT_EXIST,
    
    /**
     * 期望该行不存在。
     */
    EXPECT_NOT_EXIST;
}
