package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.NumberUtils;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.concurrent.TimeUnit;

/**
 * 表的配置选项，用于配置TTL、MaxVersions.
 * <p>TTL: TimeToLive的缩写, TableStore支持数据自动过期, TimeToLive即为数据的存活时间.</p>
 * 服务端根据当前时间, 每列每个版本的版本号, 表的TTL设置决定该列该版本是否过期, 过期的数据会自动清理.
 * <p>MaxVersions: TableStore每行每列中, 最多保存的版本数. 当写入的版本超过MaxVersions时, TableStore只保留版本号最大的MaxVersions个版本.</p>
 * <p>MaxTimeDeviation: TableStore写入数据所指定的版本与系统时间的偏差允许的最大值，不允许写入与系统偏差大于MaxTimeDeviation的数据。</p>
 */
public class TableOptions implements Jsonizable {
    /**
     * 表数据的TTL时间，单位为秒。
     * 在表创建后，该配置项可通过调用{@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}动态更改。
     */
    private OptionalValue<Integer> timeToLive = new OptionalValue<Integer>("TimeToLive");

    /**
     * 属性列的最大保留版本数。
     * 在表创建后，该配置项可通过调用{@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}动态更改。
     */
    private OptionalValue<Integer> maxVersions = new OptionalValue<Integer>("MaxVersions");

    /**
     * 指定版本写入数据时所指定的版本与系统当前时间偏差允许的最大值，单位为秒。
     * 不允许写入该偏差范围外的数据。
     * 在表创建后，该配置项可通过调用{@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}动态更改。
     */
    private OptionalValue<Long> maxTimeDeviation = new OptionalValue<Long>("MaxTimeDeviation");

    /**
     * 表上是否允许有Update操作
     * 如果设置为false，则对应表中，只能执行Put和Delete操作，不能执行Update操作
     * 在表创建后，该配置项可通过调用{@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}动态修改。
     */
    private OptionalValue<Boolean> allowUpdate = new OptionalValue<Boolean>("AllowUpdate");

    /**
     * 构造TableOptions对象。
     */
    public TableOptions() {
    }

    /**
     * 构造TableOptions对象。
     *
     * @param timeToLive TTL时间
     */
    public TableOptions(int timeToLive) {
        setTimeToLive(timeToLive);
    }

    /**
     * 构造TableOptions对象。
     *
     * @param timeToLive  TTL时间
     * @param maxVersions 最大保留版本数
     */
    public TableOptions(int timeToLive, int maxVersions) {
        setTimeToLive(timeToLive);
        setMaxVersions(maxVersions);
    }

    /**
     * 构造TableOptions对象。
     *
     * @param timeToLive  TTL时间
     * @param maxVersions 最大保留版本数
     * @param maxTimeDeviation 允许写入的指定版本与系统时间最大偏差
     */
    public TableOptions(int timeToLive, int maxVersions, long maxTimeDeviation) {
        setTimeToLive(timeToLive);
        setMaxVersions(maxVersions);
        setMaxTimeDeviation(maxTimeDeviation);
    }

    /**
     * 构造TableOptions对象
     */
     public TableOptions(boolean allowUpdate) {
         setAllowUpdate(allowUpdate);
     }

    /**
     * 获取TTL时间，单位为秒。
     *
     * @return TTL时间
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public int getTimeToLive() {
        if (!timeToLive.isValueSet()) {
            throw new IllegalStateException("The value of TimeToLive is not set.");
        }
        return timeToLive.getValue();
    }

    /**
     * 设置表数据的TTL时间，单位为秒。
     *
     * @param timeToLive TTL时间，单位为秒
     */
    public void setTimeToLive(int timeToLive) {
        Preconditions.checkArgument(timeToLive > 0 || timeToLive == -1,
                "The value of timeToLive can be -1 or any positive value.");
        this.timeToLive.setValue(timeToLive);
    }

    /**
     * 设置表数据的TTL时间
     *
     * @param days TTL时间，单位为天
     */
    public void setTimeToLiveInDays(int days) {
        Preconditions.checkArgument(days > 0 || days == -1,
                "The value of timeToLive can be -1 or any positive value.");
        if (days == -1) {
            this.timeToLive.setValue(-1);
        } else {
            long seconds = TimeUnit.DAYS.toSeconds(days);
            this.timeToLive.setValue(NumberUtils.longToInt(seconds));
        }
    }

    /**
     * 查询是否调用{@link #setTimeToLive(int)}设置了TTL。
     *
     * @return 是否有设置TTL。
     */
    public boolean hasSetTimeToLive() {
        return this.timeToLive.isValueSet();
    }

    /**
     * 获取最大版本数。
     *
     * @return 最大版本数
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public int getMaxVersions() {
        if (!maxVersions.isValueSet()) {
            throw new IllegalStateException("The value of MaxVersions is not set.");
        }
        return maxVersions.getValue();
    }

    /**
     * 设置最大版本数。
     *
     * @param maxVersions 最大版本数
     */
    public void setMaxVersions(int maxVersions) {
        Preconditions.checkArgument(maxVersions > 0, "MaxVersions must be greater than 0.");
        this.maxVersions.setValue(maxVersions);
    }

    /**
     * 查询是否调用{@link #setMaxVersions(int)}设置了MaxVerisons。
     *
     * @return 是否有设置MaxVersions。
     */
    public boolean hasSetMaxVersions() {
        return maxVersions.isValueSet();
    }

    /**
     * 获取允许的指定版本写入数据时所指定的版本与系统当前时间的最大偏差。
     *
     * @return 最大偏差
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public long getMaxTimeDeviation() {
        if (!maxTimeDeviation.isValueSet()) {
            throw new IllegalStateException("The value of MaxTimeDeviation is not set.");
        }
        return maxTimeDeviation.getValue();
    }

    /**
     * 设置允许的指定版本写入数据时所指定的版本与系统当前时间的最大偏差。
     *
     * @param maxTimeDeviation 最大偏差，单位秒
     */
    public void setMaxTimeDeviation(long maxTimeDeviation) {
        Preconditions.checkArgument(maxTimeDeviation > 0, "MaxTimeDeviation must be greater than 0.");
        this.maxTimeDeviation.setValue(maxTimeDeviation);
    }

    /**
     * 查询是否调用{@link #setMaxTimeDeviation(long)}设置了MaxTimeDeviation。
     *
     * @return 是否有设置MaxTimeDeviation。
     */
    public boolean hasSetMaxTimeDeviation() {
        return maxTimeDeviation.isValueSet();
    }

    /**
     * 设置表中的数据允许或者禁止Update操作
     * @param allowUpdate
     */
    public void setAllowUpdate(boolean allowUpdate) {
        this.allowUpdate.setValue(allowUpdate);
    }

    /**
     * 查询是否调用{@link #setAllowUpdate(boolean)}禁止/允许表中的数据有Update操作
     * @return 是否有设置AllowUpdate
     */
    public boolean hasSetAllowUpdate() { return allowUpdate.isValueSet(); }

    /**
     * 获取是否允许表中的数据上有Update操作
     * @return 是否允许Update
     * @throws java.lang.IllegalStateException 若没有配置该参数
     */
    public boolean getAllowUpdate() {
        if (!allowUpdate.isValueSet()) {
            throw new IllegalStateException("The value of AllowUpdate is not set.");
        }
        return allowUpdate.getValue();
    }

    @Override
    public String toString() {
        return timeToLive + ", " + maxVersions + ", " + maxTimeDeviation + ", " + allowUpdate;
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
        sb.append("{");
        this.jsonizeFields(sb, true);
        sb.append("}");
    }

    protected boolean jsonizeFields(StringBuilder sb, boolean firstItem) {
        if (this.timeToLive.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"TimeToLive\": ");
            sb.append(this.timeToLive.getValue());
        }
        if (this.maxVersions.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"MaxVersions\": ");
            sb.append(this.maxVersions.getValue());
        }
        if (this.maxTimeDeviation.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"MaxTimeDeviation\": ");
            sb.append(this.maxTimeDeviation.getValue());
        }
        if (this.allowUpdate.isValueSet()) {
            if (firstItem) {
                firstItem = false;
            } else {
                sb.append(", ");
            }
            sb.append("\"AllowUpdate\": ");
            sb.append(this.allowUpdate.getValue());
        }
        return firstItem;
    }

}
