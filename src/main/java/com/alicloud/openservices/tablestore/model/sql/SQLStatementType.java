package com.alicloud.openservices.tablestore.model.sql;

/**
 * 表示 SQL Statement 的数据类型。
 */
public enum SQLStatementType {
    /**
     * 查询。
     */
    SQL_SELECT,

    /**
     * 建表。
     */
    SQL_CREATE_TABLE,

    /**
     * 查询表列表。
     */
    SQL_SHOW_TABLE,

    /**
     * 查询表格式。
     */
    SQL_DESCRIBE_TABLE,

    /**
     * 删除表。
     */
    SQL_DROP_TABLE,

    /**
     * 修改表。
     */
    SQL_ALTER_TABLE,
}
