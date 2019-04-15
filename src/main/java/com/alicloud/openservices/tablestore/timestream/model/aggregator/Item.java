package com.alicloud.openservices.tablestore.timestream.model.aggregator;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.timestream.model.TimestreamIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class Item implements Cloneable {
    private String groupByValue;
    private List<String> gourpByColumn;
    private Map<String, ColumnValue> columnValueMap = new TreeMap<String, ColumnValue>();
    private TimestreamIdentifier meta;

    public Item(List<String> gourpByColumn, TimestreamIdentifier meta) {
        this.gourpByColumn = gourpByColumn;
        this.meta = meta;
    }

    public Item add(String name, ColumnValue value) {
        columnValueMap.put(name, value);
        return this;
    }

    public Item add(String name, String value) {
        columnValueMap.put(name, ColumnValue.fromString(value));
        return this;
    }

    public Item add(String name, long value) {
        columnValueMap.put(name, ColumnValue.fromLong(value));
        return this;
    }

    public ColumnValue getColumnValue(String name) {
        return columnValueMap.get(name);
    }

    public String groupBy() {
        if (groupByValue == null) {
            StringBuilder sb = new StringBuilder();
            for (String key : gourpByColumn) {
                sb.append(columnValueMap.get(key).asString());
                sb.append(',');
            }
            groupByValue = sb.toString();
        }
        return groupByValue;
    }

    public TimestreamIdentifier getMeta() {
        return meta;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<String, ColumnValue> key : columnValueMap.entrySet()) {

            switch (key.getValue().getType()) {
                case STRING:
                    sb.append(key.getKey() + ":"+ key.getValue().asString() + ", ");
                    continue;
                case DOUBLE:
                    sb.append(key.getKey() + ":"+ key.getValue().asDouble() + ", ");
                    continue;
                case INTEGER:
                    sb.append(key.getKey() + ":"+ key.getValue().asLong() + ", ");
                    continue;
                default:
                    throw new RuntimeException("not support");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public Item clone() {
        Item newItem = new Item(gourpByColumn, meta);
        for (Map.Entry<String, ColumnValue> e : this.columnValueMap.entrySet()) {
            // TODO 需要增加ColumValue的Clone方法
            ColumnValue v = e.getValue();
            switch (v.getType()) {
                case INTEGER:
                    newItem.columnValueMap.put(e.getKey(), ColumnValue.fromLong(v.asLong()));
                    continue;
                case DOUBLE:
                    newItem.columnValueMap.put(e.getKey(), ColumnValue.fromDouble(v.asDouble()));
                    continue;
                case STRING:
                    newItem.columnValueMap.put(e.getKey(), ColumnValue.fromString(v.asString()));
                    continue;
                    default:
                        throw new RuntimeException("not support");
            }

        }
        return newItem;
    }

    public static List<Item> buildItems(List<String> gourpByColumn, List<TimestreamIdentifier> metas) {
        List<Item> itemList = new ArrayList<Item>(metas.size());

        for (TimestreamIdentifier m : metas) {
            itemList.add(new Item(gourpByColumn, m));
        }

        return itemList;
    }
}
