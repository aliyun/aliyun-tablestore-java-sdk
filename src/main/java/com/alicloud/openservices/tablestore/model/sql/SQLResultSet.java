package com.alicloud.openservices.tablestore.model.sql;

/**
 * 表示 SQL 表的数据返回集合
 */
public interface SQLResultSet {

    /**
     * 返回数据集的 Schema
     *
     * @return
     */
    SQLTableMeta getSQLTableMeta();

    /**
     * 是否有下一条数据。
     * @return
     */
    boolean hasNext();

    /**
     * 返回下一条数据
     * @return
     */
    SQLRow next();

    /**
     * 返回总行数
     * @return
     */
    long rowCount();

    /**
     * 跳转到第 rowIndex 行
     *
     * @param rowIndex 列游标
     * @return 如果跳转成功返回 true，如果未成功（比如越界），返回 false
     */
    boolean absolute(int rowIndex);

}
