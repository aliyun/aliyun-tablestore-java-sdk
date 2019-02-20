package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ComputeSplitsBySizeResponse extends Response implements Jsonizable {

    /**
     * 响应所包含的数据分块信息列表，每个分块信息包含分块所在的partition位置信息以及开始和终止主键值。 每个数据分块按照主键的递增顺序排列。
     * 
     */
    private List<Split> splits = new ArrayList<Split>();

    /**
     * 此次操作消耗的能力单元。
     * 
     */
    private ConsumedCapacity consumedCapacity;

    /**
     * 表的主键定义。 字典内的主键是有顺序的，顺序与用户添加主键的顺序相同。
     * 
     */
    private List<PrimaryKeySchema> primaryKeySchema = new ArrayList<PrimaryKeySchema>();

    /**
     * internal use
     */
    public ComputeSplitsBySizeResponse(Response meta) {
        super(meta);
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{");
        sb.append("\"ConsumedCapacity\": ");
        if (this.getConsumedCapacity() != null) {
            this.getConsumedCapacity().jsonize(sb, newline + " ");
        } else {
            sb.append("null");
        }
        sb.append(", ");

        sb.append("\"PrimaryKeySchema\": ");
        boolean firstItem = true;
        if (this.getPrimaryKeySchema() != null) {
            sb.append("[ ");
            for (PrimaryKeySchema pks : this.primaryKeySchema) {
                if (firstItem == true) {
                    firstItem = false;
                } else {
                    sb.append(", ");
                }
                pks.jsonize(sb, newline + " ");
            }
            sb.append("] ");
        } else {
            sb.append("null ");
        }
        sb.append(", ");
        sb.append("\"Splits\": ");
        if (this.getSplits() != null) {
            sb.append("[");
            firstItem = true;
            for (Split s : this.splits) {
                if (firstItem == true) {
                    firstItem = false;
                } else {
                    sb.append(", ");
                }
                s.jsonize(sb, newline + " ");
            }
            sb.append("]");
        } else {
            sb.append("null ");
        }
        sb.append("}");
    }

    /**
     * 获得响应返回的数据分块列表。
     *
     */
    public List<Split> getSplits() {
        return splits;
    }

    /**
     * 设置响应的数据分块列表。
     *
     * @param splits
     *            需要设置的数据分块列表。
     */
    public void setSplits(List<Split> splits) {
        this.splits = splits;
    }

    /**
     * 添加一个数据分块。
     *
     * @param split
     *            所需要添加的数据分块。
     */
    public void addSplit(Split split) {
        this.splits.add(split);
    }

    /**
     * 获得ComputeSplitsBySize操作所消耗的CU数值。
     *
     */
    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    /**
     * 设置ComputeSplitsBySize操作所消耗的CU数值参数。
     *
     * @param consumedCapacity
     *            ComputeSplitsBySize操作所消耗的CU参数数值。
     */
    public void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        this.consumedCapacity = consumedCapacity;
    }

    /**
     * 获得主键列定义。
     *
     */
    public List<PrimaryKeySchema> getPrimaryKeySchema() {
        return primaryKeySchema;
    }

    /**
     * 设置主键列定义。
     *
     * @param primaryKeySchema
     *            主键列的定义列表。
     */
    public void setPrimaryKeySchema(List<PrimaryKeySchema> primaryKeySchema) {
        this.primaryKeySchema = primaryKeySchema;
    }

    /**
     * 添加一个主键列定义。
     *
     * @param name
     *            主键列的名称。
     * @param type
     *            主键列的数据类型。
     */
    public void addPrimaryKeySchema(String name, PrimaryKeyType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(),
                "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key should not be null.");

        this.primaryKeySchema.add(new PrimaryKeySchema(name, type));
    }

    /**
     * 添加一个主键列定义。
     *
     * @param name
     *            主键列的名称。
     * @param type
     *            主键列的数据类型。
     * @param option
     *            主键列的属性。
     */
    public void addPrimaryKeySchema(String name, PrimaryKeyType type, PrimaryKeyOption option) {
        Preconditions.checkArgument(name != null && !name.isEmpty(),
                "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key should not be null.");
        Preconditions.checkNotNull(option, "The option of primary key should not be null.");

        this.primaryKeySchema.add(new PrimaryKeySchema(name, type, option));
    }

    /**
     * 添加一个主键列定义。
     *
     * @param key
     *            主键列的定义
     */
    public void addPrimaryKeySchema(PrimaryKeySchema key) {
        Preconditions.checkNotNull(key, "The primary key schema should not be null.");
        
        this.primaryKeySchema.add(key);
    }

    /**
     * 添加一组主键列定义。
     *
     * @param pks
     *            主键列的定义
     */
    public void addPrimaryKeySchemas(List<PrimaryKeySchema> pks) {
        Preconditions.checkArgument(pks != null && !pks.isEmpty(),
                "The primary key schema should not be null or empty.");
        
        this.primaryKeySchema.addAll(pks);
    }

    /**
     * 添加一组主键列定义。
     *
     * @param pks
     *            主键列的定义
     */
    public void addPrimaryKeySchemas(PrimaryKeySchema[] pks) {
        Preconditions.checkArgument(pks != null && pks.length != 0,
                "The primary key schema should not be null or empty.");
        
        Collections.addAll(this.primaryKeySchema, pks);
    }

}
