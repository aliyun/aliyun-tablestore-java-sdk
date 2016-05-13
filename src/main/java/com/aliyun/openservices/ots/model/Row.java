package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;

import java.util.*;

/**
 * 表示查询时返回的数据行。
 * <p>
 * {@link Row#getColumns()}包含了查询时指定的返回列，可能有主键列也可能有属性列。
 * 如果查询时没有指定返回列，则包含有整行所有列的数据。
 * </p>
 */
public class Row {
    
    private SortedMap<String, ColumnValue> columns =
            new TreeMap<String, ColumnValue>(); // 行中的列

    /**
     * 获取列（Column）名称与值的只读对应字典。
     * @return 列（Column）名称与值的只读对应字典。
     * @throws ClientException 解码列值失败。
     */
    public Map<String, ColumnValue> getColumns() throws ClientException {
        return Collections.unmodifiableMap(columns);
    }

    /**
     * 获取列（Column）名称与值的只读对应字典，字典中的列按名称进行排序。
     *
     * @return 列（Column）名称与值的只读对应字典。
     * @throws ClientException 解码列值失败。
     */
    public SortedMap<String, ColumnValue> getSortedColumns() throws ClientException {
        return Collections.unmodifiableSortedMap(columns);
    }

    /**
     * internal use
     */
    public Row(){
    }

    /**
     * internal use
     */
    public void addColumn(String name, ColumnValue value) {
        columns.put(name, value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        boolean first = true;
        for (Map.Entry<String, ColumnValue> entry : columns.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append(':').append(entry.getValue().toString());
        }
        sb.append(']');
        return sb.toString();
    }
}