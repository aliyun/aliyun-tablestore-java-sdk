package com.aliyun.openservices.ots.model;

import java.util.List;

public class ListTableResult extends OTSResult {
    /**
     * 表的名称列表。
     */
    private List<String> tableNames;
    
    ListTableResult() {
    }
    
    ListTableResult(OTSResult meta) {
        super(meta);
    }

    /**
     * 获取表的名称列表。
     * @return 表的名称列表。
     */
    public List<String> getTableNames() {
        return tableNames;
    }

    void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }
}
