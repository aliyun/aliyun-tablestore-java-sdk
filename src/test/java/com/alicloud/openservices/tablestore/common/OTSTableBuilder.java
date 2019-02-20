package com.alicloud.openservices.tablestore.common;

import com.alicloud.openservices.tablestore.model.ReservedThroughput;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.TableOptions;

public class OTSTableBuilder {
    private TableMeta meta;
    private TableOptions to;
    private ReservedThroughput reservedThroughput = new ReservedThroughput();
    
    private OTSTableBuilder() {}
    
    public static OTSTableBuilder newInstance() {
        return new OTSTableBuilder();
    }
    
    public OTSTableBuilder setTableMeta(TableMeta meta) {
        this.meta = meta;
        return this;
    }
    
    public OTSTableBuilder setTableOptions(TableOptions to) {
        this.to = to;
        return this;
    }
    
    public Table toTable() {
        return new Table(meta, reservedThroughput, to);
    }
}
