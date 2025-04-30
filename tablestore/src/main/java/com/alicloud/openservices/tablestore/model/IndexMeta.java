package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.model.search.SyncStat;

import java.util.*;

/**
 * The structure information of the index table, including the name of the index table, and the definition of the primary key and predefined columns of the index table.
 */
public class IndexMeta implements Jsonizable {
    /**
     * The name of the index table.
     */
    private String indexName;

    /**
     * The primary key definition of the index table.
     * The primary keys are ordered, and the order is the same as the order in which the user adds the primary keys to the index table.
     */
    private List<String> primaryKey = new ArrayList<String>();

    /**
     * Predefined column definitions for the index table.
     */
    private List<String> definedColumns = new ArrayList<String>();

    private IndexType indexType = IndexType.IT_GLOBAL_INDEX;

    private IndexUpdateMode indexUpdateMode = IndexUpdateMode.IUM_ASYNC_INDEX;

    private SyncStat.SyncPhase indexSyncPhase;


    /**
     * Create a new <code>IndexMeta</code> instance with the given index table name.
     *
     * @param indexName The name of the index table.
     */
    public IndexMeta(String indexName) {
        Preconditions.checkArgument(indexName != null && !indexName.isEmpty(), "The name of table should not be null or empty.");
        this.indexName = indexName;
    }

    /**
     * Returns the name of the index table.
     *
     * @return The name of the index table.
     */
    public String getIndexName() {
        return indexName;
    }

    /**
     * Set the name of the index table.
     *
     * @param indexName The name of the index table.
     */
    public void setIndexName(String indexName) {
        Preconditions.checkArgument(indexName != null && !indexName.isEmpty(), "The name of index should not be null or empty.");

        this.indexName = indexName;
    }

    /**
     * Returns a read-only list containing the names of all primary key columns.
     *
     * @return A read-only list containing the names of all primary key columns.
     */
    public List<String> getPrimaryKeyList() {
        return Collections.unmodifiableList(primaryKey);
    }

    /**
     * Add a primary key column.
     * <p>The order of primary keys in the final index table will be the same as the order in which the user adds the primary keys.</p>
     *
     * @param name The name of the primary key column.
     */
    public void addPrimaryKeyColumn(String name) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of primary key should not be null or empty.");

        this.primaryKey.add(name);
    }

    /**
     * Returns a read-only list containing all predefined column names.
     *
     * @return A read-only list containing all primary key column names.
     */
    public List<String> getDefinedColumnsList() {
        return Collections.unmodifiableList(definedColumns);
    }

    /**
     * Add a predefined column.
     *
     * @param name The name of the predefined column.
     */
    public void addDefinedColumn(String name) {
        Preconditions.checkArgument(name != null && !name.isEmpty(), "The name of defined column should not be null or empty.");

        this.definedColumns.add(name);
    }

    /**
     * Get the index table type
     * @return Index table type (currently only supports GLOBAL_INDEX)
     */
    public IndexType getIndexType() {
        return indexType;
    }


    /**
     * Set the index table type
     *
     * @param type The index table type (currently only GLOBAL_INDEX is supported)
     */
    public void setIndexType(IndexType type) {
        indexType = type;
    }

    /**
     * Get the index table update mode.
     * @return Index table update mode (currently only ASYNC_INDEX is supported).
     */
    public IndexUpdateMode getIndexUpdateMode() {
        return indexUpdateMode;
    }

    /**
     * Set the index table update mode
     *
     * @param indexUpdateMode (currently only ASYNC_INDEX is supported)
     */
    public void setIndexUpdateMode(IndexUpdateMode indexUpdateMode) {
        this.indexUpdateMode = indexUpdateMode;
    }

    /**
     * Get the index table synchronization phase
     * @return Index table synchronization phase
     */
    public SyncStat.SyncPhase getIndexSyncPhase() {
        return indexSyncPhase;
    }

    /**
     * Set the index table sync phase
     *
     * @param indexSyncPhase the sync phase of the index table
     */
    public void setIndexSyncPhase(SyncStat.SyncPhase indexSyncPhase) {
        this.indexSyncPhase = indexSyncPhase;
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
