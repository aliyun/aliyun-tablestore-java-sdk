package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.*;

public class Row implements IRow {

    private PrimaryKey primaryKey;

    private Column[] columns;

    private NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap;

    /**
     * 构造函数。
     *
     * @param primaryKey 行的主键，不能为null或者为空
     * @param columns    该行的属性列，不能为null
     */
    public Row(PrimaryKey primaryKey, List<Column> columns) {
        this(primaryKey, columns.toArray(new Column[columns.size()]));
    }

    /**
     * 构造函数。
     *
     * @param primaryKey 行的主键，不能为null或者为空
     * @param columns    该行的属性列，不能为null
     */
    public Row(PrimaryKey primaryKey, Column[] columns) {
        Preconditions.checkArgument(primaryKey != null, "The primary key of row should not be null.");
        Preconditions.checkNotNull(columns, "The columns of row should not be null.");

        this.primaryKey = primaryKey;
        this.columns = columns;
        sortColumns(); // it may not been sorted, so we should sort it first
    }

    /**
     * 将数组中的所有属性列按名称升序、timestamp降序的顺序重新排列。
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
     * 二分查找指定的列.
     * @param name 要查找的列名
     * @return 如果包含查找的列, 返回对应的index; 如果不包含该列, 返回可以插入该列的位置; 如果所有元素都小于该列, 返回-1.
     */
    private int binarySearch(String name) {
        Column searchTerm = new Column(name, ColumnValue.INTERNAL_NULL_VALUE, Long.MAX_VALUE);

        // 若数组中有多列与searchTerm相同，那不保证一定返回第一列，Row中的数据是TableStore返回的，不会出现这种情况。
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
     * 获取所有的属性列。
     * <p>数组中的所有属性列按名称升序排列，相同名称的属性列按timestamp降序排列。</p>
     *
     * @return 所有属性列
     */
    public Column[] getColumns() {
        return columns;
    }

    /**
     * 获取某个特定名称的属性列的所有版本的值。
     * <p>返回结果中这些属性列按timestamp降序排列。</p>
     *
     * @param name 属性列的名称
     * @return 若该属性列存在，则返回所有版本的值，按timestamp降序排列，否则返回空列表
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
     * 获取该属性列中最新版本的值。
     *
     * @param name 属性列的名称
     * @return 若该属性列存在，则返回最新版本的值，否则返回null
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
     * 检查该行中是否有该名称的属性列。
     *
     * @param name 属性列的名称
     * @return 若存在，则返回true，否则返回false
     */
    public boolean contains(String name) {
        return getLatestColumn(name) != null;
    }

    /**
     * 检查该行是否包含属性列。
     *
     * @return 若该行不包含任何属性列，则返回true，否则返回false
     */
    public boolean isEmpty() {
        return columns == null || columns.length == 0;
    }

    /**
     * 返回一个包含所有属性列的Map。
     * <p>该Map为一个双层Map，第一层为属性列名称到所有版本属性列的映射，第二层为时间戳与属性列的映射。</p>
     * <p>属性列名称在Map中升序排列，属性列时间戳降序排列。</p>
     *
     * @return 返回包含所有属性列的map
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
