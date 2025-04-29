/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * Copyright (C) Alibaba Cloud Computing
 */

package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.model.condition.ColumnCondition;

/**
 * TableStore supports conditional update functionality, where you can set a condition {@link Condition} when performing PutRow, UpdateRow, DeleteRow, or BatchWriteRow operations.
 * A Condition includes row existence expectation {@link RowExistenceExpectation} and column conditions {@link ColumnCondition}.
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
