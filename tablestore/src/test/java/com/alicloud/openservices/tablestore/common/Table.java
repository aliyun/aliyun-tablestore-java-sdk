package com.alicloud.openservices.tablestore.common;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alicloud.openservices.tablestore.model.*;

public class Table {
    private long lastUpdateTime;
    private TableMeta meta;
    private ReservedThroughput reservedThroughput;
    private TableOptions tableOptions;
    private Map<PrimaryKey, Row> datas = new ConcurrentHashMap<PrimaryKey, Row>();
    
    public Table(
            TableMeta meta, 
            ReservedThroughput reservedThroughput,
            TableOptions tableOptions) {
        this.lastUpdateTime = new Date().getTime();
        this.meta = meta;
        this.reservedThroughput = reservedThroughput;
        this.tableOptions = tableOptions;
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    public TableMeta getMeta() {
        return meta;
    }

    public void setMeta(TableMeta meta) {
        this.meta = meta;
    }

    public ReservedThroughput getReservedThroughput() {
        return reservedThroughput;
    }

    public void setReservedThroughput(ReservedThroughput reservedThroughput) {
        this.reservedThroughput = reservedThroughput;
    }
    public TableOptions getTableOptions() {
        return tableOptions;
    }

    public void setTableOptions(TableOptions tableOptions) {
        this.tableOptions = tableOptions;
    }

    public Map<PrimaryKey, Row> getDatas() {
        return datas;
    }

    public void setDatas(Map<PrimaryKey, Row> datas) {
        this.datas = datas;
    }
   
}
