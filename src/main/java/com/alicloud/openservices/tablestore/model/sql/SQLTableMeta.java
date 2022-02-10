package com.alicloud.openservices.tablestore.model.sql;

import java.util.List;
import java.util.Map;

/**
 * SQL 表的结构信息
 */
public class SQLTableMeta {

    /**
     * 表的字段定义
     */
    private final List<SQLColumnSchema> schema;

    /**
     * 表字段名到表字段下标的映射表
     */
    private final Map<String, Integer> columnsMap;

    public SQLTableMeta(List<SQLColumnSchema> schema, Map<String, Integer> columnsMap) {
        this.schema = schema;
        this.columnsMap = columnsMap;
    }

    public List<SQLColumnSchema> getSchema() {
        return schema;
    }

    public Map<String, Integer> getColumnsMap() {
        return columnsMap;
    }

}
