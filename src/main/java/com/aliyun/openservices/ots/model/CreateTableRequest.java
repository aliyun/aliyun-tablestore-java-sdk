package com.aliyun.openservices.ots.model;

import static com.aliyun.openservices.ots.utils.CodingUtils.*;

public class CreateTableRequest {
    /**
     * 表的Meta
     */
    private TableMeta tableMeta;
    
    /**
     * 表的预留吞吐量设置。
     */
    private ReservedThroughput reservedThroughput;

    public CreateTableRequest() {
        tableMeta = new TableMeta();
    }
    
    public CreateTableRequest(TableMeta tableMeta) {
        setTableMeta(tableMeta);
    }

    /**
     * 获取表的Meta。
     * @return 表的Meta
     */
    public TableMeta getTableMeta() {
        return tableMeta;
    }
    
    /**
     * 设置表的Meta。
     * @param tableMeta 表的Meta
     */
    public void setTableMeta(TableMeta tableMeta) {
        assertParameterNotNull(tableMeta, "tableMeta");
        this.tableMeta = tableMeta;
    }
    
    /**
     * 获取表的预留吞吐量。
     * @return 表的预留吞吐量设置。
     */
    public ReservedThroughput getReservedThroughput() {
        return reservedThroughput;
    }

    /**
     * 设置表的预留吞吐量。
     * @param reservedThroughput 表的预留吞吐量。
     */
    public void setReservedThroughput(ReservedThroughput reservedThroughput) {
        assertParameterNotNull(reservedThroughput, "reservedThroughput");
        this.reservedThroughput = reservedThroughput;
    }
    
    /**
     * 设置表的预留吞吐量。
     * @param capacityUnit 表的预留吞吐量的值。
     */
    public void setReservedThroughput(CapacityUnit capacityUnit) {
        assertParameterNotNull(capacityUnit, "capacityUnit");
        this.reservedThroughput = new ReservedThroughput(capacityUnit);
    }
}
