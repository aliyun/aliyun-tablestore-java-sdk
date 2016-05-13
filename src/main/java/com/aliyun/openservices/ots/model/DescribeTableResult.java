package com.aliyun.openservices.ots.model;

public class DescribeTableResult extends OTSResult {
    /**
     * 表的Meta定义。
     */
    private TableMeta tableMeta;
    
    /**
     * 表的预留吞吐量的更改信息。
     */
    private ReservedThroughputDetails reservedThroughputDetails;

    public DescribeTableResult(OTSResult meta) {
        super(meta);
    }
    
    public void setTableMeta(TableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    /**
     * 返回表的Meta定义。
     * @return 表的Meta。
     */
    public TableMeta getTableMeta() {
        return tableMeta;
    }
    
    void setReservedThroughputDetails(ReservedThroughputDetails reservedThroughputDetails) {
        this.reservedThroughputDetails = reservedThroughputDetails;
    }
    
    /**
     * 返回表的预留吞吐量的更改信息。
     * @return 表的预留吞吐量的更改信息。
     */
    public ReservedThroughputDetails getReservedThroughputDetails() {
        return reservedThroughputDetails;
    }
}
