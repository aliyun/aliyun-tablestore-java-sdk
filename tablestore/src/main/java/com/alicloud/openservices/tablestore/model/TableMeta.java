package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.*;

/**
 * The table's structure information, including the table's name and primary key definition.
 */
public class TableMeta implements Jsonizable {
    /**
     * The name of the table.
     */
    private String tableName;

    /**
     * Definition of the table's primary key.
     * The primary keys in the dictionary are ordered, and the order is the same as the order in which the user added the primary keys.
     */
    private List<PrimaryKeySchema> primaryKey = new ArrayList<PrimaryKeySchema>();

    private Map<String, PrimaryKeySchema> primaryKeySchemaMap;

    /**
     * Predefined column definitions for the table.
     */
    private List<DefinedColumnSchema> definedColumns = new ArrayList<DefinedColumnSchema>();

    private Map<String, DefinedColumnSchema> definedColumnsSchemaMap;

    /**
     * Creates a new <code>TableMeta</code> instance with the given table name.
     *
     * @param tableName The name of the table.
     */
    public TableMeta(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    /**
     * Returns the name of the table.
     *
     * @return The name of the table.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets the name of the table.
     *
     * @param tableName The name of the table.
     */
    public void setTableName(String tableName) {
        Preconditions.checkArgument(tableName != null && !tableName.isEmpty(), "The name of table should not be null or empty.");

        this.tableName = tableName;
    }

    /**
     * Returns the dictionary corresponding to the column names and types of the primary key. 
     * The traversal order of this dictionary is consistent with the order of the primary keys in the table.
     *
     * @return A dictionary corresponding to the column names and types of the primary key.
     */
    public Map<String, PrimaryKeyType> getPrimaryKeyMap() {
        Map<String, PrimaryKeyType> result = new LinkedHashMap<String, PrimaryKeyType>();
        for (PrimaryKeySchema key : primaryKey) {
            result.put(key.getName(), key.getType());
        }
        return result;
    }

    /**
     * Returns the dictionary of correspondence between the column names and type definitions of the primary key. 
     * The traversal order of this dictionary is consistent with the order of the primary keys in the table.
     *
     * @return The dictionary of correspondence between the column names and type definitions of the primary key.
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
     * Returns a read-only list containing definitions of all primary key columns.
     *
     * @return a read-only list containing definitions of all primary key columns.
     */
    public List<PrimaryKeySchema> getPrimaryKeyList() {
        return Collections.unmodifiableList(primaryKey);
    }

    /**
     * Add a primary key column.
     * <p>The order of primary keys in the final created table will be the same as the order in which the user adds the primary keys.</p>
     *
     * @param name The name of the primary key column.
     * @param type The data type of the primary key column.
     */
    public void addPrimaryKeyColumn(String name, PrimaryKeyType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key should not be null.");

        this.primaryKey.add(new PrimaryKeySchema(name, type));
        primaryKeySchemaMap = null;
    }

    /**
     * Add a primary key column.
     * <p>The order of primary keys in the final created table will be the same as the order in which the user adds the primary keys.</p>
     *
     * @param name   The name of the primary key column.
     * @param type   The data type of the primary key column.
     * @param option The attribute of the primary key column.
     */
    public void addPrimaryKeyColumn(String name, PrimaryKeyType type, PrimaryKeyOption option) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key should not be null.");
        Preconditions.checkNotNull(option, "The option of primary key should not be null.");

        this.primaryKey.add(new PrimaryKeySchema(name, type, option));
        primaryKeySchemaMap = null;
    }

    /**
     * Add a primary key auto-increment column.
     * <p>The order of primary keys in the final created table will be the same as the order in which the user adds the primary keys.</p>
     *
     * @param name   The name of the primary key auto-increment column.
     */
    public void addAutoIncrementPrimaryKeyColumn(String name) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");

        this.primaryKey.add(new PrimaryKeySchema(name, PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT));
        primaryKeySchemaMap = null;
    }

    /**
     * Add a primary key column.
     * <p>The order of primary keys in the final created table will be the same as the order in which the user adds the primary keys.</p>
     *
     * @param key The definition of the primary key column
     */
    public void addPrimaryKeyColumn(PrimaryKeySchema key) {
        Preconditions.checkNotNull(key, "The primary key schema should not be null.");
        this.primaryKey.add(key);
        primaryKeySchemaMap = null;
    }

    /**
     * Add a set of primary key columns.
     * <p>The order of the primary keys in the final created table will be the same as the order in which the user adds the primary keys.</p>
     *
     * @param pks Definition of the primary key columns
     */
    public void addPrimaryKeyColumns(List<PrimaryKeySchema> pks) {
        Preconditions.checkArgument(pks != null && !pks.isEmpty(), "The primary key schema should not be null or empty.");
        this.primaryKey.addAll(pks);
        primaryKeySchemaMap = null;
    }

    /**
     * Add a set of primary key columns.
     * <p>The order of primary keys in the final created table will be the same as the order in which the user adds the primary keys.</p>
     *
     * @param pks Definition of the primary key columns
     */
    public void addPrimaryKeyColumns(PrimaryKeySchema[] pks) {
        Preconditions.checkArgument(pks != null && pks.length != 0, "The primary key schema should not be null or empty.");
        Collections.addAll(this.primaryKey, pks);
        primaryKeySchemaMap = null;
    }

    /**
     * Returns the dictionary of predefined column names and their corresponding types.
     *
     * @return The dictionary of predefined column names and their corresponding types.
     */
    public Map<String, DefinedColumnType> getDefinedColumnMap() {
        Map<String, DefinedColumnType> result = new LinkedHashMap<String, DefinedColumnType>();
        for (DefinedColumnSchema key : definedColumns) {
            result.put(key.getName(), key.getType());
        }
        return result;
    }

    /**
     * Returns the dictionary corresponding to the column names and type definitions of predefined columns.
     *
     * @return The dictionary corresponding to the column names and type definitions of predefined columns.
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
     * Returns a read-only list containing all predefined column definitions.
     *
     * @return a read-only list containing all predefined column definitions.
     */
    public List<DefinedColumnSchema> getDefinedColumnsList() {
        return Collections.unmodifiableList(definedColumns);
    }

    /**
     * Add a predefined column.
     *
     * @param name The name of the predefined column.
     * @param type The data type of the predefined column.
     */
    public void addDefinedColumn(String name, DefinedColumnType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of defined column should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of defined column should not be null.");

        this.definedColumns.add(new DefinedColumnSchema(name, type));
        definedColumnsSchemaMap = null;
    }

    /**
     * Add a predefined column.
     *
     * @param column The definition of the predefined column
     */
    public void addDefinedColumn(DefinedColumnSchema column) {
        Preconditions.checkNotNull(column, "The defined column schema should not be null.");
        this.definedColumns.add(column);
        definedColumnsSchemaMap = null;
    }

    /**
     * Add a set of predefined columns.
     *
     * @param columns The definition of predefined columns
     */
    public void addDefinedColumns(List<DefinedColumnSchema> columns) {
        Preconditions.checkArgument(columns != null && !columns.isEmpty(), "The defined column schema should not be null or empty.");
        this.definedColumns.addAll(columns);
        definedColumnsSchemaMap = null;
    }

    /**
     * Add a set of predefined columns.
     *
     * @param columns The definition of predefined columns
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
