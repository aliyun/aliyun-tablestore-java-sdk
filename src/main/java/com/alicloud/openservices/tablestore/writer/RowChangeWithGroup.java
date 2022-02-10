package com.alicloud.openservices.tablestore.writer;


import com.alicloud.openservices.tablestore.model.RowChange;

public class RowChangeWithGroup {
    public final RowChange rowChange;
    public final Group group;

    public RowChangeWithGroup(RowChange rowChange, Group group) {
        this.rowChange = rowChange;
        this.group = group;
    }
}
