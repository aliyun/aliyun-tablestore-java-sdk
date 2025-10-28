package com.alicloud.openservices.tablestore.model;

/**
 * UpdateGlobalTableRequest修改全局表中物理表的读写权限，需注意，这会让被禁写的物理表上的写操作失败。
 */
public class UpdateGlobalTableRequest implements Request {
    private String globalTableId;
    private String globalTableName;
    private GlobalTableTypes.UpdatePhyTable phyTable;

    public UpdateGlobalTableRequest(String globalTableId, String globalTableName, GlobalTableTypes.UpdatePhyTable phyTable) {
        this.globalTableId = globalTableId;
        this.globalTableName = globalTableName;
        this.phyTable = phyTable;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_UPDATE_GLOBAL_TABLE;
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

    public GlobalTableTypes.UpdatePhyTable getUpdatePhyTable() {
        return phyTable;
    }

    public void setUpdatePhyTable(GlobalTableTypes.UpdatePhyTable phyTable) {
        this.phyTable = phyTable;
    }
}
