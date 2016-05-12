/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

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
