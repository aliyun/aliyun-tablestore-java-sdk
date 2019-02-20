package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.CalculateHelper;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.*;

/**
 * TableStore中每行数据都包含主键({@link PrimaryKey})，
 * 主键由多列主键列构成({@link PrimaryKeyColumn})，
 * 每一个主键列包含主键列名称和主键列的值{@link PrimaryKeyValue}。
 * <p>主键中包含的主键列的个数以及主键列的顺序与创建表时TableMeta中定义的一致。</p>
 */
public class PrimaryKey implements Comparable<PrimaryKey>, Jsonizable, Measurable {
    private PrimaryKeyColumn[] primaryKey;
    private Map<String, PrimaryKeyColumn> nameMap;

    private int dataSize = -1;

    /**
     * 构造函数。
     * <p>传入的主键列数组不能为null pointer，主键列的个数以及主键列在数组中的顺序必须与创建表时TableMeta中定义的一致。</p>
     *
     * @param primaryKey 该主键包含的所有主键列
     */
    public PrimaryKey(PrimaryKeyColumn[] primaryKey) {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be null or empty.");
        this.primaryKey = primaryKey;
    }

    /**
     * 构造函数。
     * <p>传入的主键列列表不能为null pointer，主键列的个数以及主键列在列表中的顺序必须与创建表时TableMeta中定义的一致。</p>
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
     * 获取某个指定名称的主键列。
     * <p>若找不到该名称的主键列，则返回null。</p>
     * <p>若初始化时传入的主键列列表包含相同名称的主键列，则不保证返回具体哪个对应名称的主键列的值。</p>
     *
     * @param name 主键列的名称
     * @return 若存在则返回对应的主键列的值，否则返回null
     */
    public PrimaryKeyColumn getPrimaryKeyColumn(String name) {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be set before getPrimaryKeyColumn.");
        if (nameMap == null) {
            makeMap();
        }

        return nameMap.get(name);
    }

    /**
     * 按位置获取对应的主键列。
     * <p>主键中包含的主键列的个数以及主键列的顺序与创建表时TableMeta中定义的一致。</p>
     * <p>若对应的位置不存在，会抛出IllegalArgumentException异常。</p>
     *
     * @param index 主键列的位置
     * @return 具体位置对应的主键列
     */
    public PrimaryKeyColumn getPrimaryKeyColumn(int index) {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be set before getPrimaryKeyColumn.");
        if (index < 0 || index >= primaryKey.length) {
            throw new IllegalArgumentException("The index is out of bounds.");
        }

        return primaryKey[index];
    }

    /**
     * 获取所有的主键列。
     * <p>主键中包含的主键列的个数以及主键列的顺序与创建表时TableMeta中定义的一致。</p>
     *
     * @return 所有的主键列
     */
    public PrimaryKeyColumn[] getPrimaryKeyColumns() {
    	if (this.primaryKey == null) {
    		return null;
    	}
        return Arrays.copyOf(primaryKey, primaryKey.length);
    }

    /**
     * 获取主键列名称与主键列映射的Map。
     * <p>若初始化时传入的主键列列表包含相同名称的主键列，则返回的Map中只会返回其中的一列。</p>
     *
     * @return 主键列名称与主键列映射的Map
     */
    public Map<String, PrimaryKeyColumn> getPrimaryKeyColumnsMap() {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be set before getPrimaryKeyColumnsMap.");
        if (nameMap == null) {
            makeMap();
        }

        return Collections.unmodifiableMap(nameMap);
    }

    /**
     * 检查主键中是否有该名称的主键列。
     *
     * @param name 主键列的名称
     * @return 若包含则返回true，否则返回false
     */
    public boolean contains(String name) {
        Preconditions.checkArgument(primaryKey != null && primaryKey.length != 0, "The primary key should not be set.");
        if (nameMap == null) {
            makeMap();
        }

        return nameMap.containsKey(name);
    }

    /**
     * 返回主键列的个数。
     *
     * @return 主键列的个数
     */
    public int size() {
        if (primaryKey == null) {
        	return 0;
        } else {
        	return primaryKey.length;
        }
    }

    /**
     * 该主键是否为空，若主键未包含任何主键列则代表该主键为空。
     *
     * @return 若主键为空则返回true，否则返回false
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
     * 获取行主键的数据大小总和，大小总和包括所有主键列的名称和值。
     *
     * @return 行主键的数据大小总和
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
     * 比较两个主键。
     * <p>对比的两个主键必须为相同的schema，即列数、主键名称和顺序都完全一致。</p>
     *
     * @param target
     * @return 比较结果
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
