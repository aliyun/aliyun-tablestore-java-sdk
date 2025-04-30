package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.CalculateHelper;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.*;

/**
 * In TableStore, each row of data contains a primary key ({@link PrimaryKey}),
 * which is composed of multiple primary key columns ({@link PrimaryKeyColumn}).
 * Each primary key column includes the name of the primary key column and the value of the primary key column ({@link PrimaryKeyValue}).
 * <p>The number of primary key columns included in the primary key and their order are consistent with what is defined in the TableMeta when creating the table.</p>
 */
public class PrimaryKey implements Comparable<PrimaryKey>, Jsonizable, Measurable {
    private PrimaryKeyColumn[] primaryKey;
    private Map<String, PrimaryKeyColumn> nameMap;

    private int dataSize = -1;

    /**
     * Constructor.
     * <p>The primary key column array passed in must not be a null pointer. The number of primary key columns and their order in the array must be consistent with what was defined in the TableMeta when the table was created.</p>
     *
     * @param primaryKey All the primary key columns included in this primary key
     */
    public PrimaryKey(PrimaryKeyColumn[] primaryKey) {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be null or empty.");
        this.primaryKey = primaryKey;
    }

    /**
     * Constructor.
     * <p>The primary key column list passed in must not be a null pointer. The number of primary key columns and their order in the list must match the definition in TableMeta when the table was created.</p>
     *
     * @param primaryKey
     */
    public PrimaryKey(List<PrimaryKeyColumn> primaryKey) {
        Preconditions.checkArgument(primaryKey != null, "The primary key should not be null or empty.");
        this.primaryKey = primaryKey.toArray(new PrimaryKeyColumn[primaryKey.size()]);
    }
    
    /**
     * internal use
     */
    public PrimaryKey() {
    }
    
    /**
     * Get the primary key column of a specified name.
     * <p>Returns null if the primary key column with the specified name is not found.</p>
     * <p>If the list of primary key columns passed during initialization contains primary key columns with the same name, 
     * it is not guaranteed which specific primary key column value corresponding to the name will be returned.</p>
     *
     * @param name The name of the primary key column
     * @return Returns the value of the corresponding primary key column if it exists; otherwise, returns null
     */
    public PrimaryKeyColumn getPrimaryKeyColumn(String name) {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be set before getPrimaryKeyColumn.");
        if (nameMap == null) {
            makeMap();
        }

        return nameMap.get(name);
    }

    /**
     * Get the corresponding primary key column by position.
     * <p>The number of primary key columns included in the primary key and the order of the primary key columns are consistent with what is defined in TableMeta when creating the table.</p>
     * <p>If the corresponding position does not exist, an IllegalArgumentException will be thrown.</p>
     *
     * @param index The position of the primary key column
     * @return The primary key column corresponding to the specific position
     */
    public PrimaryKeyColumn getPrimaryKeyColumn(int index) {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be set before getPrimaryKeyColumn.");
        if (index < 0 || index >= primaryKey.length) {
            throw new IllegalArgumentException("The index is out of bounds.");
        }

        return primaryKey[index];
    }

    /**
     * Get all the primary key columns.
     * <p>The number of primary key columns included in the primary key and their order are consistent with the definition in TableMeta when creating the table.</p>
     *
     * @return all the primary key columns
     */
    public PrimaryKeyColumn[] getPrimaryKeyColumns() {
    	if (this.primaryKey == null) {
    		return null;
    	}
        return Arrays.copyOf(primaryKey, primaryKey.length);
    }

    /**
     * Get the Map of primary key column names to primary key columns.
     * <p>If the primary key column list passed during initialization contains primary key columns with the same name, 
     * only one of these columns will be returned in the resulting Map.</p>
     *
     * @return the Map of primary key column names to primary key columns
     */
    public Map<String, PrimaryKeyColumn> getPrimaryKeyColumnsMap() {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be set before getPrimaryKeyColumnsMap.");
        if (nameMap == null) {
            makeMap();
        }

        return Collections.unmodifiableMap(nameMap);
    }

    /**
     * Check if the primary key contains a primary key column with this name.
     *
     * @param name The name of the primary key column
     * @return Returns true if it contains, otherwise returns false
     */
    public boolean contains(String name) {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be set.");
        if (nameMap == null) {
            makeMap();
        }

        return nameMap.containsKey(name);
    }

    /**
     * Returns the number of primary key columns.
     *
     * @return the number of primary key columns
     */
    public int size() {
        if (primaryKey == null) {
        	return 0;
        } else {
        	return primaryKey.length;
        }
    }

    /**
     * Checks if the primary key is empty, meaning it does not contain any primary key columns.
     *
     * @return Returns true if the primary key is empty; otherwise, returns false.
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    private void makeMap() {
        HashMap<String, PrimaryKeyColumn> temp = new HashMap<String, PrimaryKeyColumn>(primaryKey.length);
        for (PrimaryKeyColumn column : primaryKey) {
            temp.put(column.getName(), column);
        }
        nameMap = temp;
    }

    @Deprecated
    public int getSize() {
        return getDataSize();
    }

    /**
     * Get the total data size of the row primary key, which includes the names and values of all primary key columns.
     *
     * @return the total data size of the row primary key
     */
    @Override
    public int getDataSize() {
    	if (primaryKey == null) {
    		return 0;
    	}
        if (dataSize == -1) {
            int size = 0;
            for (PrimaryKeyColumn key : primaryKey) {
                size += CalculateHelper.calcStringSizeInBytes(key.getName());
                size += key.getValue().getDataSize();
            }
            dataSize = size;
        }
        return dataSize;
    }

    /**
     * Compare two primary keys.
     * <p>The two primary keys being compared must have the same schema, meaning the number of columns, primary key names, and order must be completely consistent.</p>
     *
     * @param target
     * @return Comparison result
     */
    @Override
    public int compareTo(PrimaryKey target) {
        if (this.primaryKey.length != target.primaryKey.length) {
            throw new IllegalArgumentException("The schema of the two primary key compared is not the same.");
        }

        for (int i = 0; i < this.primaryKey.length; i++) {
            int ret = this.primaryKey[i].compareTo(target.primaryKey[i]);
            if (ret != 0) {
                return ret;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (PrimaryKeyColumn pk : this.primaryKey) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(pk.getName());
            sb.append(":");
            sb.append(pk.getValue());
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof PrimaryKey)) {
            return false;
        }

        PrimaryKey pk = (PrimaryKey) o;
        if (this.primaryKey.length != pk.primaryKey.length) {
            return false;
        }

        for (int i = 0; i < this.primaryKey.length; i++) {
            if (!this.primaryKey[i].equals(pk.primaryKey[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(primaryKey);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("[");
        Iterator<PrimaryKeyColumn> iter = Arrays.asList(primaryKey).iterator();
        if (iter.hasNext()) {
            iter.next().jsonize(sb, newline);
            while(iter.hasNext()) {
                sb.append(", ");
                iter.next().jsonize(sb, newline);
            }
        }
        sb.append("]");
    }
}
