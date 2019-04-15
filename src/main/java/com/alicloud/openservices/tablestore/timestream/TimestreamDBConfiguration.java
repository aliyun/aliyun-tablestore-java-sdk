package com.alicloud.openservices.tablestore.timestream;

import com.alicloud.openservices.tablestore.ClientException;

import java.util.concurrent.TimeUnit;

/**
 * Timestream的客户端配置。
 */
public class TimestreamDBConfiguration {

    /**
     * meta表名字
     */
    private String metaTableName;

    /**
     * 开启数据写入时后台更新时间线updateTime
     */
    private boolean dumpMeta = true;

    /**
     * 后台更新时间线updateTime的最小间隔
     */
    private long intervalDumpMeta = 10 * 60;

    /**
     * 后台缓存时间线的内存上限
     */
    private long metaCacheSize = 10L * 1024 * 1024;
    /**
     * 异步写入数据使用的{@link com.alicloud.openservices.tablestore.TableStoreWriter}执行callback的线程数
     */
    private int threadNumForWriter = 4;

    /**
     * 当前client允许写入的最大数据表的个数
     * <p>每张表异步写入都使用了一个{@link com.alicloud.openservices.tablestore.TableStoreWriter}，通过控制允许写入的数据表个数可以控制总内存</p>
     */
    private int maxDataTableNumForWrite = 63;

    /**
     * 创建TimestreamDBConfiguration
     * @param metaTableName
     */
    public TimestreamDBConfiguration(String metaTableName) {
        setMetaTableName(metaTableName);
    }

    private void setMetaTableName(String metaTableName) {
        if (metaTableName == null || metaTableName.isEmpty()) {
            throw new ClientException("Meta table name cannot be empty");
        }
        this.metaTableName = metaTableName;
    }

    /**
     * 获取meta表的表名
     * @return
     */
    public String getMetaTableName() {
        return metaTableName;
    }

    /**
     * 获取当前client允许写入的最大数据表的个数
     * @return
     */
    public int getMaxDataTableNumForWrite() {
        return maxDataTableNumForWrite;
    }

    /**
     * 设置当前client允许写入的最大数据表的个数
     * @param maxDataTableNumForWrite
     */
    public void setMaxDataTableNumForWrite(int maxDataTableNumForWrite) {
        this.maxDataTableNumForWrite = maxDataTableNumForWrite;
    }

    /**
     * 设置是否开启数据写入时后台更新时间线updateTime
     * @param dumpMeta
     */
    public void setDumpMeta(boolean dumpMeta) {
        this.dumpMeta = dumpMeta;
    }

    /**
     * 获取是否开启了数据写入时后台更新时间线
     * @return
     */
    public boolean getDumpMeta() {
        return this.dumpMeta;
    }

    /**
     * 设置后台更新时间线UpdateTime的周期，默认10分钟
     * @param interval 间隔
     * @param unit 单位
     */
    public void setIntervalDumpMeta(int interval, TimeUnit unit) {
        this.intervalDumpMeta = unit.toSeconds(interval);
    }

    /**
     * 获取后台更新时间线UpdateTime的周期
     * @param unit
     * @return
     */
    public long getIntervalDumpMeta(TimeUnit unit) {
        return unit.convert(this.intervalDumpMeta, TimeUnit.SECONDS);
    }

    /**
     * 设置后台缓存的时间线大小
     * @param metaCacheSize
     * @return
     */
    public void setMetaCacheSize(long metaCacheSize) {
        this.metaCacheSize = metaCacheSize;
    }

    /**
     * 获取后台缓存的时间线大小
     * @return
     */
    public long getMetaCacheSize() {
        return this.metaCacheSize;
    }

    /**
     * 设置异步写入数据使用的{@link com.alicloud.openservices.tablestore.TableStoreWriter}执行callback的线程数
     * @param threadNumForWriter
     */
    public void setThreadNumForWriter(int threadNumForWriter) {
        this.threadNumForWriter = threadNumForWriter;
    }

    /**
     * 获取异步写入数据使用的{@link com.alicloud.openservices.tablestore.TableStoreWriter}执行callback的线程数
     * @return
     */
    public int getThreadNumForWriter() {
        return this.threadNumForWriter;
    }
}
