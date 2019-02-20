package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.*;

/**
 * 表的结构信息，包含表的名称以及表的主键定义。
 */
public class TableMeta implements Jsonizable {
    /**
     * 表的名称。
     */
    private String tableName;

    /**
     * 表的主键定义。
     * 字典内的主键是有顺序的，顺序与用户添加主键的顺序相同。
     */
    private List<PrimaryKeySchema> primaryKey = new ArrayList<PrimaryKeySchema>();

    private Map<String, PrimaryKeySchema> primaryKeySchemaMap;

    /**
     * 表的预定义列定义。
     */
    private List<DefinedColumnSchema> definedColumns = new ArrayList<DefinedColumnSchema>();

    private Map<String, DefinedColumnSchema> definedColumnsSchemaMap;

    /**
     * 创建一个新的给定表名的<code>TableMeta</code>实例。
     *
     * @param tableName 表名。
     */
    public TableMeta(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    /**
     * 返回表的名称。
     *
     * @return 表的名称。
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置表的名称。
     *
     * @param tableName 表的名称。
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");

        this.tableName = tableName;
    }

    /**
     * 返回主键的列名与类型的对应字典，该字典的遍历顺序与表中主键的顺序一致。
     *
     * @return 主键的列名与类型对应的字典。
     */
    public Map<String, PrimaryKeyType> getPrimaryKeyMap() {
        Map<String, PrimaryKeyType> result = new LinkedHashMap<String, PrimaryKeyType>();
        for (PrimaryKeySchema key : primaryKey) {
            result.put(key.getName(), key.getType());
        }
        return result;
    }

    /**
     * 返回主键的列名与类型定义的对应字典，该字典的遍历顺序与表中主键的顺序一致。
     *
     * @return 主键的列名与类型定义对应的字典。
     */
    public Map<String, PrimaryKeySchema> getPrimaryKeySchemaMap() {
        if (primaryKeySchemaMap == null) {
            Map<String, PrimaryKeySchema> temp = new LinkedHashMap<String, PrimaryKeySchema>();
            for (PrimaryKeySchema key : primaryKey) {
                temp.put(key.getName(), key);
            }
            primaryKeySchemaMap = temp;
        }

        return Collections.unmodifiableMap(primaryKeySchemaMap);
    }

    /**
     * 返回包含所有主键列定义的只读列表。
     *
     * @return 包含所有主键列定义的只读列表。
     */
    public List<PrimaryKeySchema> getPrimaryKeyList() {
        return Collections.unmodifiableList(primaryKey);
    }

    /**
     * 添加一个主键列。
     * <p>最终创建的表中主键的顺序与用户添加主键的顺序相同。</p>
     *
     * @param name 主键列的名称。
     * @param type 主键列的数据类型。
     */
    public void addPrimaryKeyColumn(String name, PrimaryKeyType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key should not be null.");

        this.primaryKey.add(new PrimaryKeySchema(name, type));
        primaryKeySchemaMap = null;
    }

    /**
     * 添加一个主键列。
     * <p>最终创建的表中主键的顺序与用户添加主键的顺序相同。</p>
     *
     * @param name   主键列的名称。
     * @param type   主键列的数据类型。
     * @param option 主键列的属性。
     */
    public void addPrimaryKeyColumn(String name, PrimaryKeyType type, PrimaryKeyOption option) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key should not be null.");
        Preconditions.checkNotNull(option, "The option of primary key should not be null.");

        this.primaryKey.add(new PrimaryKeySchema(name, type, option));
        primaryKeySchemaMap = null;
    }

    /**
     * 添加一个主键自增列。
     * <p>最终创建的表中主键的顺序与用户添加主键的顺序相同。</p>
     *
     * @param name   主键自增列的名称。
     */
    public void addAutoIncrementPrimaryKeyColumn(String name) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");

        this.primaryKey.add(new PrimaryKeySchema(name, PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
        primaryKeySchemaMap = null;
    }

    /**
     * 添加一个主键列。
     * <p>最终创建的表中主键的顺序与用户添加主键的顺序相同。</p>
     *
     * @param key 主键列的定义
     */
    public void addPrimaryKeyColumn(PrimaryKeySchema key) {
        Preconditions.checkNotNull(key, "The primary key schema should not be null.");
        this.primaryKey.add(key);
        primaryKeySchemaMap = null;
    }

    /**
     * 添加一组主键列。
     * <p>最终创建的表中主键的顺序与用户添加主键的顺序相同。</p>
     *
     * @param pks 主键列的定义
     */
    public void addPrimaryKeyColumns(List<PrimaryKeySchema> pks) {
        Preconditions.checkArgument(pks != null && !pks.isEmpty(), "The primary key schema should not be null or empty.");
        this.primaryKey.addAll(pks);
        primaryKeySchemaMap = null;
    }

    /**
     * 添加一组主键列。
     * <p>最终创建的表中主键的顺序与用户添加主键的顺序相同。</p>
     *
     * @param pks 主键列的定义
     */
    public void addPrimaryKeyColumns(PrimaryKeySchema[] pks) {
        Preconditions.checkArgument(pks != null && pks.length != 0, "The primary key schema should not be null or empty.");
        Collections.addAll(this.primaryKey, pks);
        primaryKeySchemaMap = null;
    }

    /**
     * 返回预定义列的列名与类型的对应字典。
     *
     * @return 预定义列的列名与类型对应的字典。
     */
    public Map<String, DefinedColumnType> getDefinedColumnMap() {
        Map<String, DefinedColumnType> result = new LinkedHashMap<String, DefinedColumnType>();
        for (DefinedColumnSchema key : definedColumns) {
            result.put(key.getName(), key.getType());
        }
        return result;
    }

    /**
     * 返回预定义列的列名与类型定义的对应字典。
     *
     * @return 预定义列的列名与类型定义对应的字典。
     */
    public Map<String, DefinedColumnSchema> getDefinedColumnSchemaMap() {
        if (definedColumnsSchemaMap == null) {
            Map<String, DefinedColumnSchema> temp = new LinkedHashMap<String, DefinedColumnSchema>();
            for (DefinedColumnSchema key : definedColumns) {
                temp.put(key.getName(), key);
            }
            definedColumnsSchemaMap = temp;
        }

        return Collections.unmodifiableMap(definedColumnsSchemaMap);
    }

    /**
     * 返回包含所有预定义列定义的只读列表。
     *
     * @return 包含所有预定义列定义的只读列表。
     */
    public List<DefinedColumnSchema> getDefinedColumnsList() {
        return Collections.unmodifiableList(definedColumns);
    }

    /**
     * 添加一个预定义列。
     *
     * @param name 预定义列的名称。
     * @param type 预定义列的数据类型。
     */
    public void addDefinedColumn(String name, DefinedColumnType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of defined column should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of defined column should not be null.");

        this.definedColumns.add(new DefinedColumnSchema(name, type));
        definedColumnsSchemaMap = null;
    }

    /**
     * 添加一个预定义列。
     *
     * @param column 预定义列的定义
     */
    public void addDefinedColumn(DefinedColumnSchema column) {
        Preconditions.checkNotNull(column, "The defined column schema should not be null.");
        this.definedColumns.add(column);
        definedColumnsSchemaMap = null;
    }

    /**
     * 添加一组预定义列。
     *
     * @param columns 预定义列的定义
     */
    public void addDefinedColumns(List<DefinedColumnSchema> columns) {
        Preconditions.checkArgument(columns != null && !columns.isEmpty(), "The defined column schema should not be null or empty.");
        this.definedColumns.addAll(columns);
        definedColumnsSchemaMap = null;
    }

    /**
     * 添加一组预定义列。
     *
     * @param columns 预定义列的定义
     */
    public void addDefinedColumns(DefinedColumnSchema[] columns) {
        Preconditions.checkArgument(columns != null && columns.length != 0, "The defined column schema should not be null or empty.");
        Collections.addAll(this.definedColumns, columns);
        definedColumnsSchemaMap = null;
    }

    @Override
    public String toString() {
        String s = "TableName: " + tableName + ", PrimaryKeySchema: ";
        boolean first = true;
        for (PrimaryKeySchema pk : primaryKey) {
            if (first) {
                first = false;
            } else {
                s += ",";
            }
            s += pk.toString();
        }
        s += ", DefinedColumnSchema: ";
        first = true;
        for (DefinedColumnSchema defCol : definedColumns) {
            if (first) {
                first = false;
            } else {
                s += ",";
            }
            s += defCol.toString();
        }
        return s;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append('{');
        sb.append(newline);
        sb.append("\"TableName\": \"");
        sb.append(tableName);
        sb.append('\"');
        sb.append(",");
        sb.append(newline);
        sb.append("\"PrimaryKey\": [");
        String curNewLine = newline + "  ";
        sb.append(curNewLine);
        ListIterator<PrimaryKeySchema> iter = primaryKey.listIterator();
        if (iter.hasNext()) {
            PrimaryKeySchema pk = iter.next();
            pk.jsonize(sb, curNewLine);
        }
        for(; iter.hasNext(); ) {
            sb.append(",");
            sb.append(curNewLine);
            PrimaryKeySchema pk = iter.next();
            pk.jsonize(sb, curNewLine);
        }
        sb.append("],");
        sb.append(newline);
        sb.append("\"DefinedColumn\": [");
        sb.append(curNewLine);
        ListIterator<DefinedColumnSchema> defColIter = definedColumns.listIterator();
        if (defColIter.hasNext()) {
            DefinedColumnSchema defCol = defColIter.next();
            defCol.jsonize(sb, curNewLine);
        }
        for(; defColIter.hasNext(); ) {
            sb.append(",");
            sb.append(newline);
            DefinedColumnSchema defCol = defColIter.next();
            defCol.jsonize(sb, curNewLine);
        }
        sb.append("]}");
    }
}
