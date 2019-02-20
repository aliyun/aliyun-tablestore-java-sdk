package com.alicloud.openservices.tablestore.common;

import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OTSRowBuilder {
    
    private List<PrimaryKeyColumn> primaryKeyColumn = new ArrayList<PrimaryKeyColumn>();
    private List<Column> attrs = new ArrayList<Column>();
    
    private OTSRowBuilder() {}
    
    public static OTSRowBuilder newInstance() {
        return new OTSRowBuilder();
    }
    
    public OTSRowBuilder addPK(PrimaryKey pk) {
        primaryKeyColumn.addAll(Arrays.asList(pk.getPrimaryKeyColumns()));
        return this;
    }
    
    public OTSRowBuilder addPrimaryKeyColumn(List<PrimaryKeyColumn> columns) {
        primaryKeyColumn.addAll(columns);
        return this;
    }
    
    public OTSRowBuilder addPrimaryKeyColumn(String name, PrimaryKeyValue value) {
        primaryKeyColumn.add(new PrimaryKeyColumn(name, value));
        return this;
    }
    
    public OTSRowBuilder addAttrColumn(String name, ColumnValue value) {
        attrs.add(new Column(name, value));
        return this;
    }
    
    public OTSRowBuilder addAttrColumn(String name, ColumnValue value, long ts) {
        attrs.add(new Column(name, value, ts));
        return this;
    }
    
    public Row toRow() {
        return new Row(new PrimaryKey(primaryKeyColumn), attrs);
    }
}
