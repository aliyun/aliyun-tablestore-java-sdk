package com.alicloud.openservices.tablestore.reader;

import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Row;

public class RowReadResult {

    private boolean isSucceed = false;
    private String tableName;
    private PrimaryKey primaryKey;
    private Error error;
    private Row rowResult;
    private ConsumedCapacity consumedCapacity;

    public RowReadResult(PrimaryKey primaryKey, BatchGetRowResponse.RowResult rowResult) {
        this.isSucceed = rowResult.isSucceed();
        this.tableName = rowResult.getTableName();
        this.primaryKey = primaryKey;
        this.error = rowResult.getError();
        this.rowResult = rowResult.getRow();
        this.consumedCapacity = rowResult.getConsumedCapacity();
    }

    public boolean isSucceed() {
        return isSucceed;
    }

    public void setSucceed(boolean succeed) {
        isSucceed = succeed;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Error getError() {
        return error;
    }

    public void setError(Error error) {
        this.error = error;
    }

    public Row getRowResult() {
        return rowResult;
    }

    public void setRowResult(Row rowResult) {
        this.rowResult = rowResult;
    }

    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    public void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        this.consumedCapacity = consumedCapacity;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
    }

    @Override
    public String toString() {
        return "RowReadResult{" +
                "isSucceed=" + isSucceed +
                ", tableName='" + tableName + '\'' +
                ", primaryKey=" + primaryKey +
                ", error=" + error +
                ", rowResult=" + rowResult +
                ", consumedCapacity=" + consumedCapacity +
                '}';
    }
}
