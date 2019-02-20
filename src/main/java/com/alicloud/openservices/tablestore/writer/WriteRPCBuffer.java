package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.model.*;

public class WriteRPCBuffer {
    public WriterConfig config;
    public BatchWriteRowRequest batchWriteRowRequest;
    public int totalSize;
    public int totalRowsCount;

    public WriteRPCBuffer(WriterConfig config) {
        this.config = config;
        this.batchWriteRowRequest = new BatchWriteRowRequest();
        this.totalSize = 0;
        this.totalRowsCount = 0;
    }

    /**
     * 向RPC缓冲中添加一个RowChange，若添加成功，返回true，否则返回false。
     * 在以下情况下会添加失败：
     *  1. 如果新增这一行会导致缓冲区溢出
     *  2. 如果新增这一行会总行数会超过{@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxBatchRowsCount}限制
     *
     * @param rowChange 行数据
     * @return 是否添加成功
     */
    public boolean appendRowChange(RowChange rowChange) {
        if (totalSize + rowChange.getDataSize() > config.getMaxBatchSize()) {
            return false;
        }

        if (totalRowsCount >= config.getMaxBatchRowsCount()) {
            return false;
        }

        addRowChange(rowChange);
        this.totalSize += rowChange.getDataSize();
        this.totalRowsCount += 1;
        return true;
    }

    private void addRowChange(RowChange rowChange) {
        batchWriteRowRequest.addRowChange(rowChange);
    }

    public BatchWriteRowRequest makeRequest() {
        return batchWriteRowRequest;
    }

    public int getTotalRowsCount() {
        return totalRowsCount;
    }

    public void clear() {
        batchWriteRowRequest = new BatchWriteRowRequest();
        totalSize = 0;
        totalRowsCount = 0;
    }
}
