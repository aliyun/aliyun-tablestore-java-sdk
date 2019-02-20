package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.*;

/**
 * 索引表的结构信息，包含索引表的名称以及索引表的主键及预定义列定义。
 */
public class IndexMeta implements Jsonizable {
    /**
     * 索引表的名称。
     */
    private String indexName;

    /**
     * 索引表的主键定义。
     * 主键是有顺序的，顺序与用户添加索引表主键的顺序相同。
     */
    private List<String> primaryKey = new ArrayList<String>();

    /**
     * 索引表的预定义列定义。
     */
    private List<String> definedColumns = new ArrayList<String>();

    private IndexType indexType = IndexType.IT_GLOBAL_INDEX;

    private IndexUpdateMode indexUpdateMode = IndexUpdateMode.IUM_ASYNC_INDEX;


    /**
     * 创建一个新的给定表名的<code>IndexMeta</code>实例。
     *
     * @param indexName 索引表名。
     */
    public IndexMeta(String indexName) {
        Preconditions.checkArgument(indexName != null && !indexName.isEmpty(), "The name of table should not be null or empty.");
        this.indexName = indexName;
    }

    /**
     * 返回索引表的名称。
     *
     * @return 索引表的名称。
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * 设置索引表的名称。
     *
     * @param indexName 索引表的名称。
     */
    public void setIndexName(String indexName) {
        Preconditions.checkArgument(indexName != null && !indexName.isEmpty(), "The name of index should not be null or empty.");

        this.indexName = indexName;
    }

    /**
     * 返回包含所有主键列名字的只读列表。
     *
     * @return 包含所有主键列名字的只读列表。
     */
    public List<String> getPrimaryKeyList() {
        return Collections.unmodifiableList(primaryKey);
    }

    /**
     * 添加一个主键列。
     * <p>最终创建的索引表中主键的顺序与用户添加主键的顺序相同。</p>
     *
     * @param name 主键列的名称。
     */
    public void addPrimaryKeyColumn(String name) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");

        this.primaryKey.add(name);
    }

    /**
     * 返回包含所有预定义列名字的只读列表。
     *
     * @return 包含所有主键列名字的只读列表。
     */
    public List<String> getDefinedColumnsList() {
        return Collections.unmodifiableList(definedColumns);
    }

    /**
     * 添加一个预定义列。
     *
     * @param name 预定义列的名称。
     */
    public void addDefinedColumn(String name) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of defined column should not be null or empty.");

        this.definedColumns.add(name);
    }

    /**
     * 得到索引表类型
     * @return 索引表类型（当前只支持GLOBAL_INDEX）
     */
    public IndexType getIndexType() {
        return indexType;
    }


    /**
     * 设置索引表类型
     *
     * @param type 索引表类型（当前只支持GLOBAL_INDEX）
     */
    public void setIndexType(IndexType type) {
        indexType = type;
    }

    /**
     * 得到索引表更新模式
     * @return 索引表更新模式（当前只支持ASYNC_INDEX）
     */
    public IndexUpdateMode getIndexUpdateMode() {
        return indexUpdateMode;
    }

    /**
     * 设置索引表更新模式
     *
     * @param indexUpdateMode （当前只支持ASYNC_INDEX）
     */
    public void setIndexUpdateMode(IndexUpdateMode indexUpdateMode) {
        this.indexUpdateMode = indexUpdateMode;
    }

    @Override
    public String toString() {
        String s = "IndexName: " + indexName + ", PrimaryKeyList ";
        boolean first = true;
        ListIterator<String> pkIter = primaryKey.listIterator();
        for (; pkIter.hasNext(); ) {
            if (first) {
                first = false;
            } else {
                s += ",";
            }
            s += pkIter.next();
        }
        String defColsStr = new String();
        first = true;
        ListIterator<String> defColIter = definedColumns.listIterator();
        for (; defColIter.hasNext(); ) {
            if (first) {
                first = false;
            } else {
                defColsStr += ",";
            }
            defColsStr += defColIter.next();
        }
        s += defColsStr;
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
        sb.append("\"IndexName\": \"");
        sb.append(indexName);
        sb.append('\"');
        sb.append(",");
        sb.append(newline);
        sb.append("\"PrimaryKey\": [");
        newline += "  ";
        sb.append(newline);
        ListIterator<String> pkIter = primaryKey.listIterator();
        boolean first = true;
        for (; pkIter.hasNext(); ) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append("\"");
            sb.append(pkIter.next());
            sb.append("\"");
        }
        sb.append("],");
        sb.append(newline);
        sb.append("\"DefinedColumns\": [");
        ListIterator<String> defColIter = definedColumns.listIterator();
        first = true;
        for (; defColIter.hasNext(); ) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append("\"");
            sb.append(defColIter.next());
            sb.append("\"");
        }
        sb.append("]}");
    }
}
