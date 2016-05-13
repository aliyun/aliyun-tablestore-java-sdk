package com.aliyun.openservices.ots.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * 表示表（Table）的结构信息。
 *
 */
public class TableMeta {
    /**
     * 表的名称。
     */
    private String tableName;

    /**
     * 表的主键字典。
     * 字典内的主键是有顺序的，顺序与用户添加主键的顺序相同。
     */
    private Map<String, PrimaryKeyType> primaryKey = new LinkedHashMap<String, PrimaryKeyType>();

    /**
     * 该构造函数保留给从返回结果创建TableMeta使用，
     * 用户不能通过该构造函数构造TableMeta。
     */
    TableMeta(){

    }

    /**
     * 创建一个新的给定表名的<code>TableMeta</code>实例。
     * @param tableName 表名。
     */
    public TableMeta(String tableName){
        if (tableName == null){
            throw new NullPointerException();
        }
        this.tableName = tableName;
    }

    /**
     * 返回表的名称。
     * @return 表名。
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置表的名称。
     * @param tableName 表的名称。
     */
    public void setTableName(String tableName) {
        if (tableName == null){
            throw new NullPointerException();
        }
        this.tableName = tableName;
    }
    
    /**
     * 返回主键的列名与值的只读对应字典。
     * @return 主键的列名与值的只读对应字典。
     */
    public Map<String, PrimaryKeyType> getPrimaryKey() {
        return Collections.unmodifiableMap(primaryKey);
    }

    /**
     * 添加一个主键列，最终创建的表中主键的顺序与用户添加主键的顺序相同。
     * @param name 主键列的名称。
     * @param type 主键列的数据类型。
     */
    public void addPrimaryKeyColumn(String name, PrimaryKeyType type){
        if (name == null){
            throw new NullPointerException();
        }
        if (type == null){
            throw new NullPointerException();
        }
        this.primaryKey.put(name, type);
    }

    @Override
    public String toString() {
        String s = "TableName: " + tableName + ", PrimaryKeySchema: ";
        boolean first = true;
        for (Map.Entry<String, PrimaryKeyType> entry : primaryKey.entrySet()) {
            if (first) {
                first = false;
            } else {
                s += ",";
            }
            s += entry.getKey() + ":" + entry.getValue();
        }
        return s;
    }
}
