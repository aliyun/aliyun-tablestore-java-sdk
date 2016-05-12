/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

/**
 * 表示数据行的删除信息。
 *
 */
public class RowDeleteChange extends RowChange{
    @Override
    public int getDataSize() {
        return primaryKey.getSize();
    }

    /**
     * 构造一个新的{@link RowDeleteChange}实例。
     * @param tableName 表的名称
     */
    public RowDeleteChange(String tableName) {
        super(tableName);
    }
}
