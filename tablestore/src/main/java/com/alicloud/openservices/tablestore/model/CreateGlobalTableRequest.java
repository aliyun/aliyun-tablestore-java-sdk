package com.alicloud.openservices.tablestore.model;

import java.util.List;

import com.google.common.collect.Lists;

import static com.alicloud.openservices.tablestore.model.GlobalTableTypes.*;


/**
 * CreateGlobalTableRequest以一张已有的表为基础表，创建全局表。
 */
public class CreateGlobalTableRequest implements Request {
    private BaseTable baseTable;
    private List<Placement> placements = Lists.newArrayList();
    private SyncMode syncMode;
    private ServeMode serveMode;

    public CreateGlobalTableRequest(BaseTable baseTable, SyncMode syncMode) {
        this.baseTable = baseTable;
        this.syncMode = syncMode;
    }

    public List<Placement> addPlacement(Placement placement) {
        if (placement==null) {
            throw new IllegalArgumentException("The placement should not be null.");
        }
        placements.add(placement);
        return placements;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_CREATE_GLOBAL_TABLE;
    }

    public BaseTable getBaseTable() {
        return baseTable;
    }

    public void setBaseTable(BaseTable baseTable) {
        this.baseTable = baseTable;
    }

    public List<Placement> getPlacements() {
        return placements;
    }

    public void setPlacements(List<Placement> placements) {
        this.placements = placements;
    }

    public SyncMode getSyncMode() {
        return syncMode;
    }

    public void setSyncMode(SyncMode syncMode) {
        this.syncMode = syncMode;
    }

    public ServeMode getServeMode() {
        return serveMode;
    }

    public void setServeMode(ServeMode serveMode) {
        this.serveMode = serveMode;
    }
}
