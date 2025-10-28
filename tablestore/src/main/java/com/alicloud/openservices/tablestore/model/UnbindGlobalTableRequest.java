package com.alicloud.openservices.tablestore.model;

import java.util.List;

import com.alicloud.openservices.tablestore.model.GlobalTableTypes.Removal;
import com.google.common.collect.Lists;


/**
 * UnbindGlobalTableRequest从全局表中解绑指定的物理表，解绑操作不会删除该物理表。
 */
public class UnbindGlobalTableRequest implements Request {
    private String globalTableId;
    private String globalTableName;
    private List<Removal> removals = Lists.newArrayList();

    public UnbindGlobalTableRequest(String globalTableId, String globalTableName) {
        this.globalTableId = globalTableId;
        this.globalTableName = globalTableName;
    }

    public List<Removal> addRemoval(Removal removal) {
        if (removal == null) {
            throw new IllegalArgumentException("removal is null");
        }
        removals.add(removal);
        return removals;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UNBIND_GLOBAL_TABLE;
    }

    public String getGlobalTableId() {
        return globalTableId;
    }

    public void setGlobalTableId(String globalTableId) {
        this.globalTableId = globalTableId;
    }

    public String getGlobalTableName() {
        return globalTableName;
    }

    public void setGlobalTableName(String globalTableName) {
        this.globalTableName = globalTableName;
    }

    public List<Removal> getRemovals() {
        return removals;
    }

    public void setRemovals(List<Removal> removals) {
        this.removals = removals;
    }
}
