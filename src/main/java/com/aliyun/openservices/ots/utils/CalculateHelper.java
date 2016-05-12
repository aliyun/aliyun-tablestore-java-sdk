package com.aliyun.openservices.ots.utils;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Map.Entry;

import com.aliyun.openservices.ots.model.ColumnValue;
import com.aliyun.openservices.ots.model.PrimaryKeyValue;
import com.aliyun.openservices.ots.model.RowDeleteChange;
import com.aliyun.openservices.ots.model.RowPrimaryKey;
import com.aliyun.openservices.ots.model.RowPutChange;
import com.aliyun.openservices.ots.model.RowUpdateChange;

public class CalculateHelper {
    
    /**
     * 计算字符串的大小(按照UTF-8编码)
     * @param str
     * @return 返回字符串的字节数
     * @throws IllegalStateException
     */
    public static int getStringDataSize(String str) throws IllegalStateException {
        try {
            return str.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e.getMessage(), e.getCause());
        }
    }
    
    /**
     * 计算一个PK列的数据大小
     * @param name pk列的名字
     * @param value pk列的值
     * @return 主键的数据大小
     * @throws IllegalStateException
     */
    public static int getPrimaryKeyDataSize(String name, PrimaryKeyValue value) throws IllegalStateException {
        int primaryKeyTotalSize = getStringDataSize(name);
        switch (value.getType()) {
            case INTEGER:
                primaryKeyTotalSize += 8;
                break;
            case STRING:
                primaryKeyTotalSize += getStringDataSize(value.asString());
                break;
            default:
                throw new IllegalStateException("Bug: not support the type : " + value.getType());
                
        }
        return primaryKeyTotalSize;
    }
    
    /**
     * 计算RowPrimaryKey的数据大小
     * @param primaryKey
     * @return 主键的数据大小
     * @throws IllegalStateException
     */
    public static int getPrimaryKeyDataSize(RowPrimaryKey primaryKey) throws IllegalStateException {
        int primaryKeyTotalSize = 0;
        // PrimaryKeys Total Size
        for (Entry<String, PrimaryKeyValue> pk : primaryKey.getPrimaryKey().entrySet()) {
            primaryKeyTotalSize += getPrimaryKeyDataSize(pk.getKey(), pk.getValue());
        }
        return primaryKeyTotalSize;
    }
    
    /**
     * 计算一个属性列的数据大小
     * @param name 属性列的名字
     * @param value 属性列的值
     * @return 属性列数据大小
     * @throws IllegalStateException
     */
    public static int getAttributeColumnDataSize(String name, ColumnValue value) throws IllegalStateException {
        int columnTotalSize = getStringDataSize(name);
        switch (value.getType()) {
            case BINARY:
                columnTotalSize += value.asBinary().length;
                break;
            case BOOLEAN:
                columnTotalSize += 1; 
                break;
            case DOUBLE:
                columnTotalSize += 8; 
                break;
            case INTEGER:
                columnTotalSize += 8;
                break;
            case STRING:
                columnTotalSize += getStringDataSize(value.asString());
                break;
            default:
                throw new IllegalStateException("Bug: not support the type : " + value.getType());
        }
        
        return columnTotalSize; 
    }
    
    /**
     * 计算属性列的数据大小
     * @param columns
     * @return 属性列数据大小
     * @throws IllegalStateException
     */
    public static int getAttributeColumnDataSize(Map<String, ColumnValue> columns) throws IllegalStateException {
        int columnTotalSize = 0;
        
        // Columns Total Size
        for (Entry<String, ColumnValue> column : columns.entrySet()) {
            columnTotalSize += getAttributeColumnDataSize(column.getKey(), column.getValue());
        }
        
        return columnTotalSize; 
    }
    
    /**
     * 计算RowPutChange的数据大小
     * @param change
     * @return RowPutChange数据大小
     * @throws IllegalStateException
     */
    public static int getRowChangeDataSize(RowPutChange change) throws IllegalStateException {
        return getPrimaryKeyDataSize(change.getRowPrimaryKey()) + getAttributeColumnDataSize(change.getAttributeColumns());
    }
    
    /**
     * 计算RowUpdateChange的数据大小
     * @param change
     * @return RowUpdateChange数据大小
     * @throws IllegalStateException
     */
    public static int getRowChangeDataSize(RowUpdateChange change) throws IllegalStateException {
        return getPrimaryKeyDataSize(change.getRowPrimaryKey()) + getAttributeColumnDataSize(change.getAttributeColumns());
    }
    
    /**
     * 计算RowDeleteChange的数据大小
     * @param change
     * @return RowDeleteChange数据大小
     * @throws IllegalStateException
     */
    public static int getRowChangeDataSize(RowDeleteChange change) throws IllegalStateException {
        return getPrimaryKeyDataSize(change.getRowPrimaryKey());
    }
}
