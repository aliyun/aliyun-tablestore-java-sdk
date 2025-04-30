package com.alicloud.openservices.tablestore.timeserieswriter.config;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSDispatchMode;
import com.alicloud.openservices.tablestore.timeserieswriter.enums.TSWriteMode;
import com.alicloud.openservices.tablestore.writer.enums.WriterRetryStrategy;

public class TimeseriesWriterConfig {

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
    private int concurrency = 64;

    /**
     * The size of the memory buffer queue for a TableStoreWriter, which must be a power of 2.
     */
    private int bufferSize = 1024; // 1024 rows

    private int flushInterval = 10000; // milliseconds

    private int logInterval = 10000; // milliseconds

    /**
     * Multi-bucket distribution mode: default partition key hash modulo
     */
    private TSDispatchMode dispatchMode = TSDispatchMode.HASH_PRIMARY_KEY;

    /**
     * Writing mode: Default concurrent writing
     */
    private TSWriteMode writeMode = TSWriteMode.PARALLEL;



    /**
     * Number of buckets for time-series data
     * Under the timeseries model, 4 or 8 buckets work better.
     * Default value: 4
     */
    private int bucketCount = 4;

    /**
     * Takes effect when the internal thread pool is built (released internally within the thread pool).
     * Thread pool for running callbacks, compute-intensive.
     * Default value: number of cores + 1.
     */
    private int callbackThreadCount = Runtime.getRuntime().availableProcessors() + 1;

    /**
     * Takes effect when the internal thread pool is built (released internally within the thread pool)
     * Thread pool for running callbacks
     * Default value: 1024
     */
    private int callbackThreadPoolQueueSize = 1024;

    /**
     * Takes effect for internal Client (Client is built and released internally)
     * The retry policy used when building the internal Client
     * Default value: No retry for specific ErrorCode
     */
    private WriterRetryStrategy writerRetryStrategy = WriterRetryStrategy.CERTAIN_ERROR_CODE_NOT_RETRY;

    /**
     * Effective for internal Client (built and released internally by the Client)
     * The maximum number of connections configured when building the internal Client is used.
     * Default value: 300
     */
    private int clientMaxConnections = 300;

    /**
     * In batch requests, writing to the same timeline is allowed.
     * true: Allowed (default)
     * false: Prohibited
     */
    private boolean allowDuplicatedRowInBatchRequest = true;



    public TimeseriesWriterConfig() {
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
    public void setConcurrency(int concurrency) {
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

    public int getBucketCount() {
        return bucketCount;
    }

    public void setBucketCount(int bucketCount) {
        Preconditions.checkArgument(bucketCount > 0, "The bulk count should be greater than 0.");
        this.bucketCount = bucketCount;
    }

    public TSDispatchMode getDispatchMode() {
        return dispatchMode;
    }

    public void setDispatchMode(TSDispatchMode dispatchMode) {
        Preconditions.checkArgument(dispatchMode != null, "The dispatch mode should be null");
        this.dispatchMode = dispatchMode;
    }

    public TSWriteMode getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(TSWriteMode writeMode) {
        Preconditions.checkArgument(writeMode != null, "The write mode should be null");
        this.writeMode = writeMode;
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
