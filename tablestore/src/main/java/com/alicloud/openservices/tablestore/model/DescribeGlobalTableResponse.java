package com.alicloud.openservices.tablestore.model;

import java.util.List;

import com.alicloud.openservices.tablestore.model.GlobalTableTypes.GlobalTableStatus;
import com.alicloud.openservices.tablestore.model.GlobalTableTypes.PhyTable;

/**
 * DescribeGlobalTable操作的返回结果。
 */
public class DescribeGlobalTableResponse extends Response {
    private String globalTableId;
    private GlobalTableTypes.GlobalTableStatus status;
    private List<PhyTable> phyTables;
    public DescribeGlobalTableResponse(Response meta) {
        super(meta);
    }

    public String getGlobalTableId() {
        return globalTableId;
    }

    public void setGlobalTableId(String globalTableId) {
        this.globalTableId = globalTableId;
    }

    public GlobalTableStatus getStatus() {
        return status;
    }

    public void setStatus(GlobalTableStatus status) {
        this.status = status;
    }

    public List<PhyTable> getPhyTables() {
        return phyTables;
    }

    public void setPhyTables(List<PhyTable> phyTables) {
        this.phyTables = phyTables;
    }
}
