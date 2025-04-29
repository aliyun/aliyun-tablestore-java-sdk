/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 */

package com.alicloud.openservices.tablestore.model;

public enum RowExistenceExpectation {
    /**
     * Makes no judgment on whether the row exists.
     */
    IGNORE,
    
    /**
     * Expect the row to exist.
     */
    EXPECT_EXIST,
    
    /**
     * Expect this row not to exist.
     */
    EXPECT_NOT_EXIST;
}
