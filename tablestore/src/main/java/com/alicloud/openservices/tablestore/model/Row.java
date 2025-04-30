package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.*;

public class Row implements IRow {

    private PrimaryKey primaryKey;

    private Column[] columns;

    private NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap;

    /**
     * Constructor.
     *
     * @param primaryKey The primary key of the row, must not be null or empty
     * @param columns    The attribute columns of this row, must not be null
     */
    public Row(PrimaryKey primaryKey, List<Column> columns) {
        this(primaryKey, columns.toArray(new Column[columns.size()]));
    }

    /**
     * Constructor.
     *
     * @param primaryKey The primary key of the row, cannot be null or empty
     * @param columns    The attribute columns of this row, cannot be null
     * @param needSortColumns Whether the attribute columns need to be sorted
     */
    public Row(PrimaryKey primaryKey, List<Column> columns, boolean needSortColumns) {
        this(primaryKey, columns.toArray(new Column[columns.size()]), needSortColumns);
    }

    /**
     * Constructor.
     *
     * @param primaryKey The primary key of the row, cannot be null or empty
     * @param columns    The attribute columns of this row, cannot be null
     */
    public Row(PrimaryKey primaryKey, Column[] columns) {
        this(primaryKey, columns, true);
    }

    public Row(PrimaryKey primaryKey, Column[] columns, boolean needSortColumns) {
        Preconditions.checkArgument(primaryKey != null, "The primary key of row should not be null.");
        Preconditions.checkNotNull(columns, "The columns of row should not be null.");

        this.primaryKey = primaryKey;
        this.columns = columns;
        if (needSortColumns) {
            sortColumns(); // it may not been sorted, so we should sort it first
        }
    }

    /**
     * Reorders all attribute columns in the array by name in ascending order and by timestamp in descending order.
     */
    private void sortColumns() {
        // check if it is already sorted, optimized as in most time it is sorted.
        boolean sorted = true;
        for (int i = 0; i < columns.length - 1; i++) {
            int ret = Column.NAME_TIMESTAMP_COMPARATOR.compare(columns[i], columns[i + 1]);
            if (ret > 0) {
                sorted = false;
                break;
            }
        }

        if (!sorted) {
            Arrays.sort(this.columns, Column.NAME_TIMESTAMP_COMPARATOR);
        }
    }

    /**
     * Binary search for the specified column.
     * @param name The column name to search for
     * @return If the column is found, return the corresponding index; if the column is not included, return the position where it can be inserted; if all elements are less than the column, return -1.
     */
    private int binarySearch(String name) {
        Column searchTerm = new Column(name, ColumnValue.INTERNAL_NULL_VALUE, Long.MAX_VALUE);

        // If there are multiple columns in the array that are the same as searchTerm, it is not guaranteed that the first column will be returned. The data in the Row is returned by TableStore, so this situation will not occur.
        // pos === ( -(insertion point) - 1)
        int pos = Arrays.binarySearch(columns, searchTerm, Column.NAME_TIMESTAMP_COMPARATOR);

        if (pos < 0) {
            pos = (pos + 1) * -1;
        }

        if (pos == columns.length) {
            return -1;
        }
        return pos;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Get all attribute columns.
     * <p>All attribute columns in the array are sorted in ascending order by name, and attribute columns with the same name are sorted in descending order by timestamp.</p>
     *
     * @return all attribute columns
     */
    public Column[] getColumns() {
        return columns;
    }

    /**
     * Get all version values of a specific attribute column with a given name.
     * <p>The returned results will list these attribute columns in descending order by timestamp.</p>
     *
     * @param name the name of the attribute column
     * @return If the attribute column exists, return all version values in descending order by timestamp; otherwise, return an empty list.
     */
    public List<Column> getColumn(String name) {
        List<Column> result = new ArrayList<Column>();

        if (columns == null || columns.length == 0) {
            return result;
        }

        int pos = binarySearch(name);
        if (pos == -1) {
            return result;
        }

        for (int i = pos; i < columns.length; i++) {
            Column col = columns[i];
            if (col.getName().equals(name)) {
                result.add(col);
            } else {
                break;
            }
        }
        return result;
    }

    /**
     * Get the value of the latest version in this attribute column.
     *
     * @param name the name of the attribute column
     * @return If the attribute column exists, return the value of the latest version; otherwise, return null.
     */
    public Column getLatestColumn(String name) {
        if (columns == null || columns.length == 0) {
            return null;
        }

        int pos = binarySearch(name);
        if (pos == -1) {
            return null;
        }

        Column col = columns[pos];

        if (col.getName().equals(name)) {
            return col;
        } else {
            return null;
        }
    }

    /**
     * Check if there is a property column with this name in this row.
     *
     * @param name The name of the property column
     * @return If it exists, return true; otherwise, return false
     */
    public boolean contains(String name) {
        return getLatestColumn(name) != null;
    }

    /**
     * Check if the row contains any attribute columns.
     *
     * @return Returns true if the row does not contain any attribute columns, otherwise returns false.
     */
    public boolean isEmpty() {
        return columns == null || columns.length == 0;
    }

    /**
     * Returns a Map containing all attribute columns.
     * <p>This Map is a two-level Map, the first level maps attribute column names to all versioned attribute columns, and the second level maps timestamps to attribute columns.</p>
     * <p>Attribute column names are sorted in ascending order in the Map, while attribute column timestamps are sorted in descending order.</p>
     *
     * @return Returns a map containing all attribute columns
     */
    public NavigableMap<String, NavigableMap<Long, ColumnValue>> getColumnsMap() {
        if (columnsMap != null) {
            return columnsMap;
        }

        columnsMap = new TreeMap<String, NavigableMap<Long, ColumnValue>>();

        if (isEmpty()) {
            return columnsMap;
        }

        for (Column col : this.columns) {
            NavigableMap<Long, ColumnValue> tsMap = columnsMap.get(col.getName());

            if (tsMap == null) {
                tsMap = new TreeMap<Long, ColumnValue>(new Comparator<Long>() {
                    public int compare(Long l1, Long l2) {
                        return l2.compareTo(l1);
                    }
                });
                columnsMap.put(col.getName(), tsMap);
            }

            tsMap.put(col.getTimestamp(), col.getValue());
        }
        return columnsMap;
    }

    @Override
    public int compareTo(IRow o) {
        return this.primaryKey.compareTo(o.getPrimaryKey());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[PrimaryKey:]");
        sb.append(this.primaryKey);
        sb.append("\n[Columns:]");
        for (Column column : this.getColumns()) {
            sb.append("(");
            sb.append(column);
            sb.append(")");
        }
        return sb.toString();
    }
}
