package com.alicloud.openservices.tablestore.model.sql;

/**
 * 表示 SQL 数据存储的数据集
 **/
public interface SQLRows {

    /**
     * 返回数据集的 Schema
     *
     * @return 数据集的 Schema
     */
    SQLTableMeta getSQLTableMeta();

    /**
     * 返回数据集的总行数
     *
     * @return 总行数
     */
    long rowCount();

    /**
     * 返回数据集的列个数
     *
     * @return 总行数
     */
    long columnCount();

    /**
     * 按行游标和列游标查询某行某列的数据
     *
     * @param rowIndex 行游标
     * @param columnIndex 列游标
     * @return 数据
     */
    Object get(int rowIndex, int columnIndex);

}
