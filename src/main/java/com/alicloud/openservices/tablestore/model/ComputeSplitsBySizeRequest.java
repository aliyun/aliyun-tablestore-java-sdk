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
     * 获得进行该类所进行的操作的接口名称。
     *
     */
    @Override
    public String getOperationName() {
        return OperationNames.OP_COMPUTE_SPLITS_BY_SIZE;
    }

    /**
     * 获得进行ComputeSplitsBySize操作的目标表格名称。
     *
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 设置进行ComputeSplitsBySize操作的目标表格名称。
     *
     * @param tableName
     *            进行ComputeSplitsBySize操作的目标表格名称。
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 获得进行ComputeSplitsBySize操作的每个数据分块的大小上限。
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
     * 设置进行ComputeSplitsBySize操作的每个数据分块的大小上限。
     *
     * @param splitSizeIn100MB
     *            进行ComputeSplitsBySize操作的每个数据分块的大小上限。
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
     * 设置splitPointLimit
     *
     * @param splitPointLimit
     */
    public void setSplitPointLimit(int splitPointLimit) {
        Preconditions.checkArgument(splitPointLimit > 0, "The value of SplitPointLimit must be greater than 0.");
        this.splitPointLimit.setValue(splitPointLimit);
    }

    /**
     * 获取设置过的splitPointLimit。
     *
     * @return splitPointLimit
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public int getSplitPointLimit() {
        if (!this.splitPointLimit.isValueSet()) {
            throw new IllegalStateException("The value of SplitPointLimit is not set.");
        }
        return this.splitPointLimit.getValue();
    }

    /**
     * 查询是否设置了splitPointLimit。
     *
     * @return 若设置过splitPointLimit，则返回true，否则返回false。
     */
    public boolean hasSetSplitPointLimit() {
        return splitPointLimit.isValueSet();
    }
}
