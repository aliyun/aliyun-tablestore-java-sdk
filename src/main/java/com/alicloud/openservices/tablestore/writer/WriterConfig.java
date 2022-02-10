package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.alicloud.openservices.tablestore.writer.enums.WriterRetryStrategy;

public class WriterConfig {

    /**
     * 主键列的值的最大大小，单位Byte。
     */
    private int maxPKColumnSize = 1024; // 1KB

    /**
     * 属性列的值的最大大小，单位Byte。
     */
    private int maxAttrColumnSize = 2 * 1024 * 1024; // 2MB

    /**
     * 一行最大的列数限制。
     */
    private int maxColumnsCount = 128;

    /**
     * 一次批量RPC请求导入的最大行数
     */
    private int maxBatchRowsCount = 200;

    /**
     * 一次批量RPC请求导入的最大数据量
     */
    private int maxBatchSize = 4 * 1024 * 1024; // 4MB

    /**
     * 一个TableStoreWriter的最大请求并发数
     */
    private int concurrency = 10;

    /**
     * 一个TableStoreWriter在内存中缓冲队列的大小，必须是2的指数。
     */
    private int bufferSize = 1024; // 1024 rows

    private int flushInterval = 10000; // milliseconds

    private int logInterval = 10000; // milliseconds

    /**
     * 是否开启SDK层的schema检查
     */
    private boolean enableSchemaCheck = true;

    /**
     * 多桶分发模式：默认分区键哈希后取模
     * */
    private DispatchMode dispatchMode = DispatchMode.HASH_PARTITION_KEY;

    /**
     * 写入模式：默认并发写
     * */
    private WriteMode writeMode = WriteMode.PARALLEL;

    /**
     * 请求构建类型：默认BatchWriteRowRequest
     * */
    private BatchRequestType batchRequestType = BatchRequestType.BATCH_WRITE_ROW;

    /**
     * 分筒数，默认参数暂定3，
     * 并发写：3个桶性能较好
     * 按序写：桶数与写入正相关
     * */
    private int bucketCount = 3;

    /**
     * 内部构建线程池时生效（线程池内部释放）
     * Callback运行线程池，计算密集型
     * 默认值：核数 + 1
     * */
    private int callbackThreadCount = Runtime.getRuntime().availableProcessors() + 1;

    /**
     * 内部构建线程池时生效（线程池内部释放）
     * Callback运行线程池
     * 默认值：1024
     * */
    private int callbackThreadPoolQueueSize = 1024;

    /**
     * 内部Client时生效（Client内部构建与释放）
     * 内部构建Client是使用的重试策略
     * 默认值：特定ErrorCode不做重试
     * */
    private WriterRetryStrategy writerRetryStrategy = WriterRetryStrategy.CERTAIN_ERROR_CODE_NOT_RETRY;

    /**
     * 内部Client时生效（Client内部构建与释放）
     * 内部构建Client是使用的最大连接数配置
     * 默认值：300
     * */
    private int clientMaxConnections = 300;

    /**
     * 批量请求中，允许重复行，若含二级索引，忽略用户设置，禁止重复false
     * true：允许（默认）
     * false：禁止
     * */
    private boolean allowDuplicatedRowInBatchRequest = true;



    public WriterConfig() {}

    /**
     * 获取主键列的值的最大大小限制，单位Byte。
     *
     * @return 主键列的值的最大大小限制。
     */
    public int getMaxPKColumnSize() {
        return maxPKColumnSize;
    }

    /**
     * 设置主键列的值的最大大小限制，单位Byte。
     *
     * @param maxPKColumnSize 主键列的值的最大大小限制。
     */
    public void setMaxPKColumnSize(int maxPKColumnSize) {
        Preconditions.checkArgument(maxPKColumnSize > 0, "The max PKColumnSize should be greater than 0.");
        this.maxPKColumnSize = maxPKColumnSize;
    }

    /**
     * 获取属性列的值的最大大小限制，单位Byte。
     *
     * @return 属性列的值的最大大小限制。
     */
    public int getMaxAttrColumnSize() {
        return maxAttrColumnSize;
    }

    /**
     * 设置属性列的值的最大大小限制，单位Byte。
     *
     * @param maxAttrColumnSize 属性列的值的最大大小限制。
     */
    public void setMaxAttrColumnSize(int maxAttrColumnSize) {
        Preconditions.checkArgument(maxAttrColumnSize > 0, "The max AttrColumnSize should be greater than 0.");
        this.maxAttrColumnSize = maxAttrColumnSize;
    }

    /**
     * 获取一行的最大列数限制。
     *
     * @return 一行的最大列数限制。
     */
    public int getMaxColumnsCount() {
        return maxColumnsCount;
    }

    /**
     * 设置一行的最大列数限制。
     *
     * @param maxColumnsCount 一行的最大列数限制。
     */
    public void setMaxColumnsCount(int maxColumnsCount) {
        Preconditions.checkArgument(maxColumnsCount > 0, "The max ColumnsCount should be greater than 0.");
        this.maxColumnsCount = maxColumnsCount;
    }

    /**
     * 获取一次批量RPC请求导入的最大行数。
     *
     * @return 一次批量RPC请求导入的最大行数。
     */
    public int getMaxBatchRowsCount() {
        return maxBatchRowsCount;
    }

    /**
     * 设置一次批量RPC请求导入的最大行数。
     *
     * @param maxBatchRowsCount 一次批量RPC请求导入的最大行数。
     */
    public void setMaxBatchRowsCount(int maxBatchRowsCount) {
        Preconditions.checkArgument(maxBatchRowsCount > 0, "The max BatchRowsCount should be greater than 0.");
        this.maxBatchRowsCount = maxBatchRowsCount;
    }

    /**
     * 获取一次批量RPC请求导入的最大数据量，单位Byte。
     *
     * @return 一次批量RPC请求导入的最大数据量。
     */
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    /**
     * 设置一次批量RPC请求导入的最大数据量，单位Byte。
     *
     * @param maxBatchSize 一次批量RPC请求导入的最大数据量。
     */
    public void setMaxBatchSize(int maxBatchSize) {
        Preconditions.checkArgument(maxBatchSize > 0, "The max BatchSize should be greater than 0.");
        this.maxBatchSize = maxBatchSize;
    }

    /**
     * 获取一个TableStoreWriter的最大请求并发数。
     *
     * @return 一个TableStoreWriter的最大请求并发数。
     */
    public int getConcurrency() {
        return concurrency;
    }

    /**
     * 设置一个TableStoreWriter的最大请求并发数。
     *
     * @param concurrency 一个TableStoreWriter的最大请求并发数。
     */
    public void setConcurrency(int concurrency){
        Preconditions.checkArgument(concurrency > 0, "The concurrency should be greater than 0.");
        this.concurrency = concurrency;
    }

    /**
     * 获取一个TableStoreWriter在内存中缓冲队列的大小，必须是2的指数。
     *
     * @return 一个TableStoreWriter在内存中缓冲队列的大小。
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * 设置一个TableStoreWriter在内存中缓冲队列的大小。
     *
     * @param bufferSize 一个TableStoreWriter在内存中缓冲队列的大小。
     */
    public void setBufferSize(int bufferSize) {
        Preconditions.checkArgument(bufferSize > 0, "The buffer size should be greater than 0.");
        this.bufferSize = bufferSize;
    }

    /**
     * 设置writer自动flush的时间间隔，单位为毫秒。
     *
     * @return writer自动flush的时间间隔。
     */
    public int getFlushInterval() {
        return flushInterval;
    }

    /**
     * 获取writer自动flush的时间间隔，单位为毫秒。
     *
     * @param flushInterval writer自动flush的时间间隔。
     */
    public void setFlushInterval(int flushInterval) {
        Preconditions.checkArgument(flushInterval > 0, "The flush interval should be greater than 0.");
        this.flushInterval = flushInterval;
    }

    /**
     * 获取是否开启在SDK层开启写入数据时的schema检查
     *
     * @return 是否开启在SDK层写入数据时的schema检查
     */
    public boolean isEnableSchemaCheck() {
        return enableSchemaCheck;
    }

    /**
     * 设置是否开启在SDK层写入数据时的schema检查
     *
     * @param enableSchemaCheck 是否开启在SDK层写入数据时的schema检查
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
