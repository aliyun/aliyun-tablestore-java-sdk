package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;

import java.util.concurrent.TimeUnit;

/**
 * Time range class, used for timeline search and data query, [begin, end), left-closed right-open
 */
public class TimeRange {
    private long begin = 0;
    private long end = Long.MAX_VALUE;

    private TimeRange() {}

    /**
     * All
     * @return
     */
    public static TimeRange all() {
        TimeRange timeRange = new TimeRange();
        return timeRange;
    }

    /**
     * Specify a certain time range
     * @param begin Start timestamp
     * @param end End timestamp
     * @param unit Time unit
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
     * Recently
     * @param time
     * @param unit Time unit
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
     * Time range before a certain time point
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
     * Time range after a certain point in time
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
