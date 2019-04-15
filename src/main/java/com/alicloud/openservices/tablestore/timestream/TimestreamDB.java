package com.alicloud.openservices.tablestore.timestream;

import com.alicloud.openservices.tablestore.timestream.model.AttributeIndexSchema;

import java.util.List;

public interface TimestreamDB {

    /**
     * 关闭client，释放资源
     * <p>请确保在所有请求执行完毕之后释放资源。释放资源之后将不能再发送请求，正在执行的请求可能无法返回结果。</p>
     */
    public void close();

    /**
     * 创建meta表，不为attributes创建索引
     */
    public void createMetaTable();

    /**
     * 创建meta表，为指定的attributes创建索引
     * <p>attribute不能为保留字段（h、n、t、s）</p>
     */
    public void createMetaTable(List<AttributeIndexSchema> indexForAttributes);

    /**
     * 删除meta表
     */
    public void deleteMetaTable();

    /**
     * 创建数据表
     * @param tableName 数据表表名
     */
    public void createDataTable(String tableName);

    /**
     * 删除数据表
     * @param tableName 数据表表名
     */
    public void deleteDataTable(String tableName);

    /**
     * 获取meta表的操作对象
     * @return
     */
    public TimestreamMetaTable metaTable();

    /**
     * 获取数据表的操作对象
     * @param tableName 数据表表名
     * @return
     */
    public TimestreamDataTable dataTable(String tableName);
}