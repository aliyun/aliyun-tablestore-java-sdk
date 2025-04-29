package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class ComputeSplitsBySizeRequest implements Request {

    private String tableName = null;

    private long splitUnitCount = 0l;
    private long splitUnitSizeInByte = 100 * 1024 * 1024; // default 100M split unit
    private OptionalValue<Integer> splitPointLimit = new OptionalValue<Integer>("SplitPointLimit");

    public ComputeSplitsBySizeRequest() {
        this(null, 0l);
    }

    public ComputeSplitsBySizeRequest(ComputeSplitsBySizeRequest request) {
        this(request.getTableName(), request.getSplitSizeIn100MB());
    }

    public ComputeSplitsBySizeRequest(String tableName, long splitSizeIn100MB) {
        this.tableName = tableName;
        this.splitUnitCount = splitSizeIn100MB;
        this.splitUnitSizeInByte = 100 * 1024 * 1024;
    }

    public ComputeSplitsBySizeRequest(String tableName, long splitUnitCount, long splitUnitSizeInByte) {
        this.tableName = tableName;
        this.splitUnitCount = splitUnitCount;
        this.splitUnitSizeInByte = splitUnitSizeInByte;
    }

    /**
     * Get the name of the interface for the operations performed by this class.
     *
     */
    @Override
    public String getOperationName() {
        return OperationNames.OP_COMPUTE_SPLITS_BY_SIZE;
    }

    /**
     * Get the name of the target table for performing the ComputeSplitsBySize operation.
     *
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Set the target table name for the ComputeSplitsBySize operation.
     *
     * @param tableName
     *            The target table name for the ComputeSplitsBySize operation.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Get the upper limit of the size for each data split when performing the ComputeSplitsBySize operation.
     *
     */
    public long getSplitSizeIn100MB() {
        return this.splitUnitCount * this.splitUnitSizeInByte / (100 * 1024 * 1024);
    }

    public long getSplitUnitCount() {
        return this.splitUnitCount;
    }
    public long getSplitUnitSizeInByte() {
        return this.splitUnitSizeInByte;
    }

    /**
     * Set the upper limit of the size for each data block during the ComputeSplitsBySize operation.
     *
     * @param splitSizeIn100MB
     *            The upper limit of the size for each data block during the ComputeSplitsBySize operation, in units of 100 MB.
     */
    public void setSplitSizeIn100MB(long splitSizeIn100MB) {
        this.splitUnitCount = splitSizeIn100MB;
        this.splitUnitSizeInByte = 100 * 1024 * 1024;
    }

    public void setSplitSizeInByte(long splitUnitCount, long splitUnitSizeInByte) {
        this.splitUnitCount = splitUnitCount;
        this.splitUnitSizeInByte = splitUnitSizeInByte;
    }

    /**
     * Set splitPointLimit
     *
     * @param splitPointLimit
     */
    public void setSplitPointLimit(int splitPointLimit) {
        Preconditions.checkArgument(splitPointLimit > 0, "The value of SplitPointLimit must be greater than 0.");
        this.splitPointLimit.setValue(splitPointLimit);
    }

    /**
     * Get the configured splitPointLimit.
     *
     * @return splitPointLimit
     * @throws java.lang.IllegalStateException if this parameter is not configured
     */
    public int getSplitPointLimit() {
        if (!this.splitPointLimit.isValueSet()) {
            throw new IllegalStateException("The value of SplitPointLimit is not set.");
        }
        return this.splitPointLimit.getValue();
    }

    /**
     * Query whether the splitPointLimit has been set.
     *
     * @return Returns true if splitPointLimit has been set, otherwise returns false.
     */
    public boolean hasSetSplitPointLimit() {
        return splitPointLimit.isValueSet();
    }
}
