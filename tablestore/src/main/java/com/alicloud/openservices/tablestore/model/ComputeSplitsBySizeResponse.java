package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ComputeSplitsBySizeResponse extends Response implements Jsonizable {

    /**
     * The list of data chunk information included in the response. Each chunk contains partition location information and the start and end primary key values. 
     * Each data chunk is arranged in increasing order of the primary key.
     */
    private List<Split> splits = new ArrayList<Split>();

    /**
     * The capacity unit consumed by this operation.
     */
    private ConsumedCapacity consumedCapacity;

    /**
     * Definition of the table's primary key. The primary keys in the dictionary are ordered, and the order is the same as the order in which the user added the primary keys.
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
     * Get the list of data chunks returned in the response.
     *
     */
    public List<Split> getSplits() {
        return splits;
    }

    /**
     * Sets the data split list in the response.
     *
     * @param splits
     *            The data split list to be set.
     */
    public void setSplits(List<Split> splits) {
        this.splits = splits;
    }

    /**
     * Add a data chunk.
     *
     * @param split
     *            The data chunk to be added.
     */
    public void addSplit(Split split) {
        this.splits.add(split);
    }

    /**
     * Get the CU value consumed by the ComputeSplitsBySize operation.
     *
     */
    public ConsumedCapacity getConsumedCapacity() {
        return consumedCapacity;
    }

    /**
     * Sets the CU value parameter consumed by the ComputeSplitsBySize operation.
     *
     * @param consumedCapacity
     *            The CU value parameter consumed by the ComputeSplitsBySize operation.
     */
    public void setConsumedCapacity(ConsumedCapacity consumedCapacity) {
        this.consumedCapacity = consumedCapacity;
    }

    /**
     * Get the primary key column definition.
     *
     */
    public List<PrimaryKeySchema> getPrimaryKeySchema() {
        return primaryKeySchema;
    }

    /**
     * Set the primary key column definition.
     *
     * @param primaryKeySchema
     *            The list of primary key column definitions.
     */
    public void setPrimaryKeySchema(List<PrimaryKeySchema> primaryKeySchema) {
        this.primaryKeySchema = primaryKeySchema;
    }

    /**
     * Add a primary key column definition.
     *
     * @param name
     *            The name of the primary key column.
     * @param type
     *            The data type of the primary key column.
     */
    public void addPrimaryKeySchema(String name, PrimaryKeyType type) {
        Preconditions.checkArgument(name != null && !name.isEmpty(),
                "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key should not be null.");

        this.primaryKeySchema.add(new PrimaryKeySchema(name, type));
    }

    /**
     * Add a primary key column definition.
     *
     * @param name
     *            The name of the primary key column.
     * @param type
     *            The data type of the primary key column.
     * @param option
     *            The attribute of the primary key column.
     */
    public void addPrimaryKeySchema(String name, PrimaryKeyType type, PrimaryKeyOption option) {
        Preconditions.checkArgument(name != null && !name.isEmpty(),
                "The name of primary key should not be null or empty.");
        Preconditions.checkNotNull(type, "The type of primary key should not be null.");
        Preconditions.checkNotNull(option, "The option of primary key should not be null.");

        this.primaryKeySchema.add(new PrimaryKeySchema(name, type, option));
    }

    /**
     * Add a primary key column definition.
     *
     * @param key
     *            the definition of the primary key column
     */
    public void addPrimaryKeySchema(PrimaryKeySchema key) {
        Preconditions.checkNotNull(key, "The primary key schema should not be null.");
        
        this.primaryKeySchema.add(key);
    }

    /**
     * Add a set of primary key column definitions.
     *
     * @param pks
     *            the definition of primary key columns
     */
    public void addPrimaryKeySchemas(List<PrimaryKeySchema> pks) {
        Preconditions.checkArgument(pks != null && !pks.isEmpty(),
                "The primary key schema should not be null or empty.");
        
        this.primaryKeySchema.addAll(pks);
    }

    /**
     * Add a set of primary key column definitions.
     *
     * @param pks
     *            the definitions of primary key columns
     */
    public void addPrimaryKeySchemas(PrimaryKeySchema[] pks) {
        Preconditions.checkArgument(pks != null && pks.length != 0,
                "The primary key schema should not be null or empty.");
        
        Collections.addAll(this.primaryKeySchema, pks);
    }

}
