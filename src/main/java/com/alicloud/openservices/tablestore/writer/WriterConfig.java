package com.alicloud.openservices.tablestore.writer;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

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

    /**
     * 是否开启SDK层的schema检查
     */
    private boolean enableSchemaCheck = true;

    public WriterConfig() {

    }

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
}
