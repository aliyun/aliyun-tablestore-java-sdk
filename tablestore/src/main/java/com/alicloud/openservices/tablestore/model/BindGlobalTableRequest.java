package com.alicloud.openservices.tablestore.model;

import java.util.List;

import com.alicloud.openservices.tablestore.model.GlobalTableTypes.Placement;
import com.google.common.collect.Lists;

/**
 * BindGlobalTableRequest在指定的位置给全局表添加一个新的物理表副本。
 */
public class BindGlobalTableRequest implements Request {
    private String globalTableId;
    private String globalTableName;
    private List<Placement> placements = Lists.newArrayList();

    public BindGlobalTableRequest(String globalTableId, String globalTableName) {
        this.globalTableId = globalTableId;
        this.globalTableName = globalTableName;
    }

    public List<Placement> addPlacement(Placement placement) {
        if (placement == null) {
            throw new IllegalArgumentException("The placement should not be null.");
        }
        placements.add(placement);
        return placements;
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

    public List<Placement> getPlacements() {
        return placements;
    }

    public void setPlacements(List<Placement> placements) {
        this.placements = placements;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_BIND_GLOBAL_TABLE;
    }
}
