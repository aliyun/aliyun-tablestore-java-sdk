package com.alicloud.openservices.tablestore.model;

/**
 * DescribeGlobalTableRequest获取全局表的物理表信息和状态。
 */
public class DescribeGlobalTableRequest implements Request {
    private String globalTableId;
    private String globalTableName;
    private GlobalTableTypes.PhyTable phyTable;
    private boolean returnRpo;


    public DescribeGlobalTableRequest(String globalTableId, String globalTableName) {
        this.globalTableId = globalTableId;
        this.globalTableName = globalTableName;
    }
    public DescribeGlobalTableRequest(String globalTableName, String regionId, String instanceName) {
        this.globalTableName = globalTableName;
        this.phyTable = new GlobalTableTypes.PhyTable(regionId, instanceName, globalTableName, false);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_DESCRIBE_GLOBAL_TABLE;
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

    public GlobalTableTypes.PhyTable getPhyTable() {
        return phyTable;
    }

    public void setPhyTable(GlobalTableTypes.PhyTable phyTable) {
        this.phyTable = phyTable;
    }

    public boolean isReturnRpo() {
        return returnRpo;
    }

    public void setReturnRpo(boolean returnRpo) {
        this.returnRpo = returnRpo;
    }
}
