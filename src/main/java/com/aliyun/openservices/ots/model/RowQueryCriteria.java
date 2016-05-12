/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.model.condition.ColumnCondition;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;
import static com.aliyun.openservices.ots.utils.CodingUtils.assertStringNotNullOrEmpty;


/**
 * 表示返回行的查询条件。
 *
 */
public class RowQueryCriteria {
    /**
     * 查询的表的名称。
     */
    private String tableName;
    
    /**
     * 要查询的列的名称，若未指定查询的列，则查询整行。
     */
    private List<String> columnsToGet = new LinkedList<String>();

    /**
     * 本次查询使用的Filter
     */
    private ColumnCondition filter;

    /**
     * 构造一个在给定名称的表中查询的条件。
     * @param tableName 查询的表名。
     */
    public RowQueryCriteria(String tableName){
        setTableName(tableName);
    }

    /**
     * 返回查询的表名。
     * @return 查询的表名。
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置查询的表名。
     * @param tableName  查询的表名。
     */
    public void setTableName(String tableName) {
        assertStringNotNullOrEmpty(tableName, "tableName");
        this.tableName = tableName;
    }
    
    /**
     * 返回返回列的名称的列表（只读）。
     * @return 列的名称的列表（只读）。
     */
    public List<String> getColumnsToGet() {
        return Collections.unmodifiableList(columnsToGet);
    }

    /**
     * 添加要返回的列。
     * @param columnName 要返回列的名称。
     */
    public void addColumnsToGet(String columnName){
        assertStringNotNullOrEmpty(columnName, "columnName");
        this.columnsToGet.add(columnName);
    }
    /**
     * 添加要返回的列。
     * @param columnNames 要返回列的名称。
     */
    public void addColumnsToGet(String[] columnNames){
        assertParameterNotNull(columnNames, "columnNames");
        for(int i = 0; i < columnNames.length; ++i){
            this.columnsToGet.add(columnNames[i]);
        }
    }

    /**
     * 设置需要读取的列的列表。若List为空，则读取所有列。
     * @param columnsToGet 需要读取的列的列表。
     */
    public void setColumnsToGet(List<String> columnsToGet) {
        assertParameterNotNull(columnsToGet, "columnsToGet");
        this.columnsToGet = columnsToGet;
    }

    /**
     * 获取本次查询使用的Filter。
     * @return 本次查询使用的Filter，若没有设置，返回null。
     */
    public ColumnCondition getFilter() {
        return filter;
    }

    /**
     * 设置本次查询使用的Filter。
     * @param filter
     */
    public void setFilter(ColumnCondition filter) {
        assertParameterNotNull(filter, "filter");
        this.filter = filter;
    }

    public void copyTo(RowQueryCriteria target) {
        target.tableName = tableName;
        target.columnsToGet.addAll(columnsToGet);
        target.filter = filter;
    }
}
