package com.alicloud.openservices.tablestore.model.sql;

import java.util.List;
import java.util.Map;

/**
 * Structural information of the SQL table
 */
public class SQLTableMeta {

    /**
     * Table field definition
     */
    private final List<SQLColumnSchema> schema;

    /**
     * Mapping table from table field names to table field indices
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
