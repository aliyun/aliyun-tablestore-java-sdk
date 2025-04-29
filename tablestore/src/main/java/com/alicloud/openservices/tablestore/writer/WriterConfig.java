package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.alicloud.openservices.tablestore.writer.enums.WriterRetryStrategy;

public class WriterConfig {

    /**
     * The maximum size of the primary key column value, in Bytes.
     */
    private int maxPKColumnSize = 1024; // 1KB

    /**
     * The maximum size of the attribute column value, in Bytes.
     */
    private int maxAttrColumnSize = 2 * 1024 * 1024; // 2MB

    /**
     * The maximum column count limit for a row.
     */
    private int maxColumnsCount = 128;

    /**
     * The maximum number of rows that can be imported in a single batch RPC request.
     */
    private int maxBatchRowsCount = 200;

    /**
     * The maximum amount of data that can be imported in a single batch RPC request.
     */
    private int maxBatchSize = 4 * 1024 * 1024; // 4MB

    /**
     * The maximum request concurrency for a TableStoreWriter
     */
    private int concurrency = 10;

    /**
     * The size of the memory buffer queue for a TableStoreWriter, which must be a power of 2.
     */
    private int bufferSize = 1024; // 1024 rows

    private int flushInterval = 10000; // milliseconds

    private int logInterval = 10000; // milliseconds

    /**
     * Whether to enable schema check at the SDK level
     */
    private boolean enableSchemaCheck = true;

    /**
     * Multi-bucket distribution mode: Default partition key hash modulo
     */
    private DispatchMode dispatchMode = DispatchMode.HASH_PARTITION_KEY;

    /**
     * Writing mode: Default concurrent writing
     */
    private WriteMode writeMode = WriteMode.PARALLEL;

    /**
     * Request construction type: default BatchWriteRowRequest
     * */
    private BatchRequestType batchRequestType = BatchRequestType.BATCH_WRITE_ROW;

    /**
     * Number of buckets, the default parameter is temporarily set to 3.
     * For concurrent writing: 3 buckets provide better performance.
     * For sequential writing: the number of buckets is positively correlated with the writing process.
     */
    private int bucketCount = 3;

    /**
     * Takes effect when internally building a thread pool (released internally within the thread pool)
     * Callback execution thread pool, computation-intensive
     * Default value: number of cores + 1
     */
    private int callbackThreadCount = Runtime.getRuntime().availableProcessors() + 1;

    /**
     * Takes effect when internally building a thread pool (released internally within the thread pool)
     * Callback execution thread pool
     * Default value: 1024
     */
    private int callbackThreadPoolQueueSize = 1024;

    /**
     * Takes effect for internal Client (Client is built and released internally)
     * The retry strategy used when building the internal Client
     * Default value: no retry for specific ErrorCodes
     */
    private WriterRetryStrategy writerRetryStrategy = WriterRetryStrategy.CERTAIN_ERROR_CODE_NOT_RETRY;

    /**
     * Takes effect for internal Client (Client is built and released internally)
     * The maximum number of connections configured when building the internal Client
     * Default value: 300
     */
    private int clientMaxConnections = 300;

    /**
     * In batch requests, duplicate rows are allowed. If it contains secondary indexes, the user settings will be ignored and duplicates are prohibited when set to false.
     * true: Allowed (default)
     * false: Prohibited
     */
    private boolean allowDuplicatedRowInBatchRequest = true;



    public WriterConfig() {}

    /**
     * Get the maximum size limit of the primary key column value in bytes.
     *
     * @return The maximum size limit of the primary key column value.
     */
    public int getMaxPKColumnSize() {
        return maxPKColumnSize;
    }

    /**
     * Set the maximum size limit for the value of the primary key column, in units of Byte.
     *
     * @param maxPKColumnSize The maximum size limit for the value of the primary key column.
     */
    public void setMaxPKColumnSize(int maxPKColumnSize) {
        Preconditions.checkArgument(maxPKColumnSize > 0, "The max PKColumnSize should be greater than 0.");
        this.maxPKColumnSize = maxPKColumnSize;
    }

    /**
     * Get the maximum size limit of the attribute column value, in units of Byte.
     *
     * @return The maximum size limit of the attribute column value.
     */
    public int getMaxAttrColumnSize() {
        return maxAttrColumnSize;
    }

    /**
     * Set the maximum size limit for the value of the attribute column, in units of Byte.
     *
     * @param maxAttrColumnSize The maximum size limit for the value of the attribute column.
     */
    public void setMaxAttrColumnSize(int maxAttrColumnSize) {
        Preconditions.checkArgument(maxAttrColumnSize > 0, "The max AttrColumnSize should be greater than 0.");
        this.maxAttrColumnSize = maxAttrColumnSize;
    }

    /**
     * Get the maximum number of columns per row limit.
     *
     * @return The maximum number of columns per row limit.
     */
    public int getMaxColumnsCount() {
        return maxColumnsCount;
    }

    /**
     * Set the maximum column count limit for a row.
     *
     * @param maxColumnsCount The maximum column count limit for a row.
     */
    public void setMaxColumnsCount(int maxColumnsCount) {
        Preconditions.checkArgument(maxColumnsCount > 0, "The max ColumnsCount should be greater than 0.");
        this.maxColumnsCount = maxColumnsCount;
    }

    /**
     * Get the maximum number of rows for a single batch RPC request import.
     *
     * @return The maximum number of rows for a single batch RPC request import.
     */
    public int getMaxBatchRowsCount() {
        return maxBatchRowsCount;
    }

    /**
     * Set the maximum number of rows for a single batch RPC request.
     *
     * @param maxBatchRowsCount The maximum number of rows for a single batch RPC request.
     */
    public void setMaxBatchRowsCount(int maxBatchRowsCount) {
        Preconditions.checkArgument(maxBatchRowsCount > 0, "The max BatchRowsCount should be greater than 0.");
        this.maxBatchRowsCount = maxBatchRowsCount;
    }

    /**
     * Get the maximum data size for a single batch RPC request, in units of Byte.
     *
     * @return The maximum data size for a single batch RPC request.
     */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /**
     * Set the maximum data size for a single batch RPC request, in units of Byte.
     *
     * @param maxBatchSize The maximum data size for a single batch RPC request.
     */
    public void setMaxBatchSize(int maxBatchSize) {
        Preconditions.checkArgument(maxBatchSize > 0, "The max BatchSize should be greater than 0.");
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * Get the maximum request concurrency for a TableStoreWriter.
     *
     * @return The maximum request concurrency for a TableStoreWriter.
     */
    public int getConcurrency() {
        return concurrency;
    }

    /**
     * Sets the maximum request concurrency for a TableStoreWriter.
     *
     * @param concurrency The maximum request concurrency for a TableStoreWriter.
     */
    public void setConcurrency(int concurrency){
        Preconditions.checkArgument(concurrency > 0, "The concurrency should be greater than 0.");
        this.concurrency = concurrency;
    }

    /**
     * Get the size of the memory buffer queue for a TableStoreWriter, which must be a power of 2.
     *
     * @return The size of the memory buffer queue for a TableStoreWriter.
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Set the size of the buffer queue in memory for a TableStoreWriter.
     *
     * @param bufferSize The size of the buffer queue in memory for a TableStoreWriter.
     */
    public void setBufferSize(int bufferSize) {
        Preconditions.checkArgument(bufferSize > 0, "The buffer size should be greater than 0.");
        this.bufferSize = bufferSize;
    }

    /**
     * Set the automatic flush time interval for the writer, in milliseconds.
     *
     * @return The automatic flush time interval for the writer.
     */
    public int getFlushInterval() {
        return flushInterval;
    }

    /**
     * Get the automatic flush interval of the writer, in milliseconds.
     *
     * @param flushInterval The automatic flush interval of the writer.
     */
    public void setFlushInterval(int flushInterval) {
        Preconditions.checkArgument(flushInterval > 0, "The flush interval should be greater than 0.");
        this.flushInterval = flushInterval;
    }

    /**
     * Get whether the schema check is enabled at the SDK level when writing data.
     *
     * @return Whether the schema check is enabled at the SDK level when writing data.
     */
    public boolean isEnableSchemaCheck() {
        return enableSchemaCheck;
    }

    /**
     * Set whether to enable schema checking at the SDK level when writing data
     *
     * @param enableSchemaCheck Whether to enable schema checking at the SDK level when writing data
     */
    public void setEnableSchemaCheck(boolean enableSchemaCheck) {
        this.enableSchemaCheck = enableSchemaCheck;
    }

    public int getBucketCount() {
        return bucketCount;
    }

    public void setBucketCount(int bucketCount) {
        Preconditions.checkArgument(bucketCount > 0, "The bulk count should be greater than 0.");
        this.bucketCount = bucketCount;
    }

    public DispatchMode getDispatchMode() {
        return dispatchMode;
    }

    public void setDispatchMode(DispatchMode dispatchMode) {
        Preconditions.checkArgument(dispatchMode != null, "The dispatch mode should be null");
        this.dispatchMode = dispatchMode;
    }

    public WriteMode getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(WriteMode writeMode) {
        Preconditions.checkArgument(writeMode != null, "The write mode should be null");
        this.writeMode = writeMode;
    }

    public BatchRequestType getBatchRequestType() {
        return batchRequestType;
    }

    public void setBatchRequestType(BatchRequestType batchRequestType) {
        Preconditions.checkArgument(batchRequestType != null, "The batch request type should be null");
        this.batchRequestType = batchRequestType;
    }

    public int getLogInterval() {
        return logInterval;
    }

    public void setLogInterval(int logInterval) {
        Preconditions.checkArgument(logInterval > 0, "The LogInterval should be greater than 0.");
        this.logInterval = logInterval;
    }

    public int getCallbackThreadCount() {
        return callbackThreadCount;
    }

    public void setCallbackThreadCount(int callbackThreadCount) {
        Preconditions.checkArgument(callbackThreadCount > 0, "The CallbackThreadCount should be greater than 0.");
        this.callbackThreadCount = callbackThreadCount;
    }

    public int getCallbackThreadPoolQueueSize() {
        return callbackThreadPoolQueueSize;
    }

    public void setCallbackThreadPoolQueueSize(int callbackThreadPoolQueueSize) {
        Preconditions.checkArgument(callbackThreadPoolQueueSize > 0, "The CallbackThreadPoolQueueSize should be greater than 0.");
        this.callbackThreadPoolQueueSize = callbackThreadPoolQueueSize;
    }

    public WriterRetryStrategy getWriterRetryStrategy() {
        return writerRetryStrategy;
    }

    public void setWriterRetryStrategy(WriterRetryStrategy writerRetryStrategy) {
        Preconditions.checkArgument(writerRetryStrategy != null, "The WriterRetryStrategy should not be null.");
        this.writerRetryStrategy = writerRetryStrategy;
    }

    public int getClientMaxConnections() {
        return clientMaxConnections;
    }

    public void setClientMaxConnections(int clientMaxConnections) {
        Preconditions.checkArgument(clientMaxConnections > 0, "The ClientMaxConnect should be greater than 0.");
        this.clientMaxConnections = clientMaxConnections;
    }

    public boolean isAllowDuplicatedRowInBatchRequest() {
        return allowDuplicatedRowInBatchRequest;
    }

    public void setAllowDuplicatedRowInBatchRequest(boolean allowDuplicatedRowInBatchRequest) {
        this.allowDuplicatedRowInBatchRequest = allowDuplicatedRowInBatchRequest;
    }
}
