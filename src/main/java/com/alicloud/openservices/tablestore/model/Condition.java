/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;

/**
 * TableStore支持条件更新功能, 在进行PutRow, UpdateRow, DeleteRow或BatchWriteRow操作时, 可以设置条件{@link Condition}.
 * Condition包括行存在性条件{@link RowExistenceExpectation}和列条件{@link ColumnCondition}.
 */
public class Condition {
    private RowExistenceExpectation rowExistenceExpectation = RowExistenceExpectation.IGNORE;
    private ColumnCondition columnCondition = null;
    
    public Condition() {
        
    }
    
    public Condition(RowExistenceExpectation rowExistenceExpectation) {
        this.rowExistenceExpectation = rowExistenceExpectation;
    }

    public RowExistenceExpectation getRowExistenceExpectation() {
        return rowExistenceExpectation;
    }

    public void setRowExistenceExpectation(
            RowExistenceExpectation rowExistenceExpectation) {
        this.rowExistenceExpectation = rowExistenceExpectation;
    }

    public ColumnCondition getColumnCondition() {
        return columnCondition;
    }

    public void setColumnCondition(ColumnCondition columnCondition) {
        this.columnCondition = columnCondition;
    }
}
