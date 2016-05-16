package com.aliyun.openservices.ots.internal.writer;

import com.aliyun.openservices.ots.model.BatchWriteRowRequest;
import com.aliyun.openservices.ots.model.RowChange;
import com.aliyun.openservices.ots.model.RowPrimaryKey;

import java.util.HashSet;
import java.util.Set;

public class WriteRPCBuffer {
    public WriterConfig config;
    public Set<RowPrimaryKey> primaryKeySet;
    public BatchWriteRowRequest batchWriteRowRequest;
    public int totalSize;
    public int totalRowsCount;

    public WriteRPCBuffer(WriterConfig config) {
        this.config = config;
        this.primaryKeySet = new HashSet<RowPrimaryKey>();
        this.batchWriteRowRequest = new BatchWriteRowRequest();
        this.totalSize = 0;
        this.totalRowsCount = 0;
    }

    /**
     * 向RPC缓冲中添加一个RowChange，若添加成功，返回true，否则返回false。
     * 在以下情况下会添加失败：
     *  1. RPCBuffer中已经包含相同主键的行
     *  2. 如果新增这一行会导致缓冲区溢出
     *  3. 如果新增这一行会总行数会超过{@link com.aliyun.openservices.ots.internal.writer.WriterConfig#maxBatchRowsCount}限制
     *
     * @param rowChange 行数据
     * @return 是否添加成功
     */
    public boolean appendRowChange(RowChange rowChange) {
        RowPrimaryKey primaryKey = rowChange.getRowPrimaryKey();
        if (primaryKeySet.contains(primaryKey)) {
            return false;
        }

        if (totalSize + rowChange.getDataSize() > config.getMaxBatchSize()) {
            return false;
        }

        if (totalRowsCount >= config.getMaxBatchRowsCount()) {
            return false;
        }

        primaryKeySet.add(primaryKey);
        batchWriteRowRequest.addRowChange(rowChange);
        this.totalSize += rowChange.getDataSize();
        this.totalRowsCount += 1;
        return true;
    }

    public BatchWriteRowRequest makeRequest() {
        return batchWriteRowRequest;
    }

    public int getTotalSize() {
        return totalSize;
    }

    public int getTotalRowsCount() {
        return totalRowsCount;
    }

    public void clear() {
        primaryKeySet.clear();
        batchWriteRowRequest = new BatchWriteRowRequest();
        totalSize = 0;
        totalRowsCount = 0;
    }
}
