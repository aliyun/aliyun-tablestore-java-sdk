package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TimeRange {
    private long start = 0L; // inclusive
    private long end = Long.MAX_VALUE; // exclusive
    private boolean eternal = false;

    /**
     * 默认构造函数。
     * 默认的时间戳区间为[0, Long.MAX_VALUE)(范围内覆盖所有可写入的时间戳)
     */
    public TimeRange() {
        this.eternal = true;
    }

    /**
     * 构造一个时间戳区间为[start, Long.MAX_VALUE)。
     *
     * @param start 起始时间戳(inclusive)
     */
    public TimeRange(long start) {
        Preconditions.checkArgument(start >= 0, "The start timestamp should be greater than 0.");
        this.start = start;
    }

    /**
     * 构造一个时间戳区间为[start, end)。
     *
     * @param start 起始时间戳(inclusive)
     * @param end 最大时间戳(exclusive)
     */
    public TimeRange(long start, long end) {
        Preconditions.checkArgument(end > start, "end is smaller than start");
        Preconditions.checkArgument(start >= 0, "The start timestamp should be greater than 0.");

        this.start = start;
        this.end = end;
    }

    /**
     * @return 起始时间戳
     */
    public long getStart() {
        return this.start;
    }

    /**
     * @return 终止时间戳
     */
    public long getEnd() {
        return this.end;
    }

    /**
     * 检查指定时间戳是否在该时间戳区间内。
     *
     * @param timestamp 时间戳
     * @return 如果在区间内则返回true，否则返回false
     */
    public boolean withinTimeRange(long timestamp) {
        if (this.eternal) return true;
        // check if >= minStamp
        return (this.start <= timestamp && timestamp < this.end);
    }

    /**
     * 比较时间戳和时间戳范围
     *
     * @param timestamp 时间戳
     * @return -1 if timestamp is less than timerange,
     * 0 if timestamp is within timerange,
     * 1 if timestamp is greater than timerange
     */
    public int compare(long timestamp) {
        if (timestamp < this.start) {
            return -1;
        } else if (timestamp >= this.end) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * 检查该TimeRange内是否只包含一个版本。
     *
     * @return 如果只包含一个版本，则返回true，否则返回false
     */
    public boolean containsOnlyOneVersion() {
        return this.end - this.start == 1;
    }

    @Override
    public int hashCode() {
        return (int)(this.start * 31 + this.end);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof TimeRange)) {
            return false;
        }

        TimeRange target = (TimeRange) o;
        return target.start == this.start && target.end == this.end;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("start=");
        sb.append(this.start);
        sb.append(", end=");
        sb.append(this.end);
        return sb.toString();
    }
}
