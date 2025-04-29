package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class TimeRange {
    private long start = 0L; // inclusive
    private long end = Long.MAX_VALUE; // exclusive
    private boolean eternal = false;

    /**
     * Default constructor.
     * The default timestamp range is [0, Long.MAX_VALUE), which covers all writable timestamps within the range.
     */
    public TimeRange() {
        this.eternal = true;
    }

    /**
     * Constructs a timestamp range of [start, Long.MAX_VALUE).
     *
     * @param start the starting timestamp (inclusive)
     */
    public TimeRange(long start) {
        Preconditions.checkArgument(start >= 0, "The start timestamp should be greater than 0.");
        this.start = start;
    }

    /**
     * Constructs a timestamp range of [start, end).
     *
     * @param start The start timestamp (inclusive)
     * @param end The end timestamp (exclusive)
     */
    public TimeRange(long start, long end) {
        Preconditions.checkArgument(end > start, "end is smaller than start");
        Preconditions.checkArgument(start >= 0, "The start timestamp should be greater than 0.");

        this.start = start;
        this.end = end;
    }

    /**
     * @return Start timestamp
     */
    public long getStart() {
        return this.start;
    }

    /**
     * @return Termination timestamp
     */
    public long getEnd() {
        return this.end;
    }

    /**
     * Checks if the specified timestamp is within this timestamp range.
     *
     * @param timestamp the timestamp to check
     * @return true if the timestamp is within the range, otherwise false
     */
    public boolean withinTimeRange(long timestamp) {
        if (this.eternal) return true;
        // check if >= minStamp
        return (this.start <= timestamp && timestamp < this.end);
    }

    /**
     * Compare a timestamp with a timestamp range
     *
     * @param timestamp the timestamp to compare
     * @return -1 if timestamp is less than the timerange,
     * 0 if timestamp is within the timerange,
     * 1 if timestamp is greater than the timerange
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
     * Check if the TimeRange only contains one version.
     *
     * @return If it only contains one version, return true; otherwise, return false.
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
