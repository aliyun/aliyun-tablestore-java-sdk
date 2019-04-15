package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;

import java.util.concurrent.TimeUnit;

/**
 * 时间范围类，用于时间线检索和数据查询，[begin, end)，左闭右开
 */
public class TimeRange {
    private long begin = 0;
    private long end = Long.MAX_VALUE;

    private TimeRange() {}

    /**
     * 所有
     * @return
     */
    public static TimeRange all() {
        TimeRange timeRange = new TimeRange();
        return timeRange;
    }

    /**
     * 指定某个时间范围
     * @param begin 起始时间戳
     * @param end 结束时间戳
     * @param unit 时间单位
     * @return
     */
    public static TimeRange range(long begin, long end, TimeUnit unit) {
        if (begin >= end) {
            throw new ClientException("The begin must be smaller than end.");
        }
        TimeRange timeRange = new TimeRange();
        timeRange.begin = unit.toMicros(begin);
        timeRange.end = unit.toMicros(end);
        return timeRange;
    }

    /**
     * 最近一段时间
     * @param time
     * @param unit 时间单位
     * @return
     */
    public static TimeRange latest(long time, TimeUnit unit) {
        if (unit.toMicros(time) <= 0) {
            throw new ClientException("The time muse be positive.");
        }
        long now = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
        if (unit.toMicros(time) > now) {
            throw new ClientException("The time muse smaller than current time.");
        }
        TimeRange timeRange = new TimeRange();
        timeRange.begin = now - unit.toMicros(time);
        timeRange.end = unit.toMicros(now);
        return timeRange;
    }

    /**
     * 某个时间点之前的时间范围
     * @param end
     * @param unit
     * @return
     */
    public static TimeRange before(long end, TimeUnit unit) {
        if (unit.toMicros(end) <= 0) {
            throw new ClientException("The end muse be positive.");
        }
        TimeRange timeRange = new TimeRange();
        timeRange.end = unit.toMicros(end);
        return timeRange;
    }

    /**
     * 某个时间点之后的时间范围
     * @param begin
     * @param unit
     * @return
     */
    public static TimeRange after(long begin, TimeUnit unit) {
        if (unit.toMicros(begin) <= 0) {
            throw new ClientException("The begin muse be positive.");
        }
        TimeRange timeRange = new TimeRange();
        timeRange.begin = unit.toMicros(begin);
        return timeRange;
    }

    public long getBeginTime() {
        return begin;
    }

    public long getEndTime() {
        return end;
    }
}
