package com.alicloud.openservices.tablestore.reader;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowQueryCriteria;

public class PrimaryKeyWithTable {
    private String tableName;
    private PrimaryKey primaryKey;
    private RowQueryCriteria criteria;

    public PrimaryKeyWithTable(String tableName, PrimaryKey primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
    }

    public PrimaryKeyWithTable(String tableName, PrimaryKey primaryKey, RowQueryCriteria criteria) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.criteria = criteria;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    public RowQueryCriteria getCriteria() {
        return criteria;
    }

    public void setCriteria(RowQueryCriteria criteria) {
        this.criteria = criteria;
    }
}
