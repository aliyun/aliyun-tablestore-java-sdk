package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.Row;

public class RowWriteResult {
    private ConsumedCapacity consumedCapacity;

    private Row row;

    public RowWriteResult(ConsumedCapacity consumedCapacity, Row row) {
        this.consumedCapacity = consumedCapacity;
        this.row = row;
    }

    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    public void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        this.consumedCapacity = consumedCapacity;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        this.row = row;
    }
}
