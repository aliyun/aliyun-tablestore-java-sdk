package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;
import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * 时序表的配置选项，目前只用于用于配置TTL
 * <p>TTL: TimeToLive的缩写, TableStore支持数据自动过期, TimeToLive即为数据的存活时间.</p>
 * 服务端根据当前时间, 每列每个版本的版本号, 表的TTL设置决定该列该版本是否过期, 过期的数据会自动清理.
 */
public class TimeseriesTableOptions implements Jsonizable {
    /**
     * 表数据的TTL时间，单位为秒。
     * 在表创建后，该配置项可通过调用{@link com.alicloud.openservices.tablestore.SyncClient#updateTable(UpdateTableRequest)}动态更改。
     */
    private OptionalValue<Integer> timeToLive = new OptionalValue<Integer>("TimeToLive");

    /**
     * 构造TableOptions对象。
     */
    public TimeseriesTableOptions() {
    }

    /**
     * 构造TableOptions对象。
     *
     * @param timeToLive TTL时间
     */
    public TimeseriesTableOptions(int timeToLive) {
        setTimeToLive(timeToLive);
    }

    /**
     * 获取TTL时间，单位为秒。
     *
     * @return TTL时间
     * @throws IllegalStateException 若没有配置该参数
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
     * @throws IllegalStateException 若没有配置该参数
     */

    @Override
    public String toString() {
        return timeToLive.toString();
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
        return firstItem;
    }

}
