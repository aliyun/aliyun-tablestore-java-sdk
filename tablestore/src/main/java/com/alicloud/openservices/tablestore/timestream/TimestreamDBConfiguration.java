package com.alicloud.openservices.tablestore.timestream;

import com.alicloud.openservices.tablestore.ClientException;

import java.util.concurrent.TimeUnit;

/**
 * The client configuration for Timestream.
 */
public class TimestreamDBConfiguration {

    /**
     * The name of the meta table
     */
    private String metaTableName;

    /**
     * Enable the background update of the timeline updateTime when writing data
     */
    private boolean dumpMeta = true;

    /**
     * The minimum interval for updating the updateTime timeline in the background
     */
    private long intervalDumpMeta = 10 * 60;

    /**
     * The upper limit of memory for the background cache timeline
     */
    private long metaCacheSize = 10L * 1024 * 1024;
    /**
     * Number of threads used by the {@link com.alicloud.openservices.tablestore.TableStoreWriter} to execute callbacks for asynchronous data writes.
     */
    private int threadNumForWriter = 4;

    /**
     * The maximum number of tables allowed to be written by the current client.
     * <p>Each table uses a {@link com.alicloud.openservices.tablestore.TableStoreWriter} for asynchronous writing. By controlling the number of tables allowed to be written, the total memory can be managed.</p>
     */
    private int maxDataTableNumForWrite = 63;

    /**
     * Create TimestreamDBConfiguration
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
     * Get the table name of the meta table
     * @return
     */
    public String getMetaTableName() {
        return metaTableName;
    }

    /**
     * Get the maximum number of data tables allowed for writing by the current client.
     * @return
     */
    public int getMaxDataTableNumForWrite() {
        return maxDataTableNumForWrite;
    }

    /**
     * Set the maximum number of data tables allowed for writing by the current client.
     * @param maxDataTableNumForWrite
     */
    public void setMaxDataTableNumForWrite(int maxDataTableNumForWrite) {
        this.maxDataTableNumForWrite = maxDataTableNumForWrite;
    }

    /**
     * Set whether to enable background update of the updateTime timeline when writing data
     * @param dumpMeta
     */
    public void setDumpMeta(boolean dumpMeta) {
        this.dumpMeta = dumpMeta;
    }

    /**
     * Get whether the background timeline update is enabled when data is written
     * @return
     */
    public boolean getDumpMeta() {
        return this.dumpMeta;
    }

    /**
     * Set the period of the background update timeline for UpdateTime, default is 10 minutes.
     * @param interval The interval between updates.
     * @param unit The unit of the interval.
     */
    public void setIntervalDumpMeta(int interval, TimeUnit unit) {
        this.intervalDumpMeta = unit.toSeconds(interval);
    }

    /**
     * Get the cycle of backend update timeline for UpdateTime
     * @param unit
     * @return
     */
    public long getIntervalDumpMeta(TimeUnit unit) {
        return unit.convert(this.intervalDumpMeta, TimeUnit.SECONDS);
    }

    /**
     * Set the timeline size of the background cache
     * @param metaCacheSize
     * @return
     */
    public void setMetaCacheSize(long metaCacheSize) {
        this.metaCacheSize = metaCacheSize;
    }

    /**
     * Get the timeline size of the background cache
     * @return
     */
    public long getMetaCacheSize() {
        return this.metaCacheSize;
    }

    /**
     * Set the number of threads used by the {@link com.alicloud.openservices.tablestore.TableStoreWriter} to execute callbacks for asynchronous data writes.
     * @param threadNumForWriter
     */
    public void setThreadNumForWriter(int threadNumForWriter) {
        this.threadNumForWriter = threadNumForWriter;
    }

    /**
     * Get the number of threads used for executing {@link com.alicloud.openservices.tablestore.TableStoreWriter} callbacks for asynchronous data writing.
     * @return
     */
    public int getThreadNumForWriter() {
        return this.threadNumForWriter;
    }
}
