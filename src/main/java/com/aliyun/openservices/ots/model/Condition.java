package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.model.condition.ColumnCondition;

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
