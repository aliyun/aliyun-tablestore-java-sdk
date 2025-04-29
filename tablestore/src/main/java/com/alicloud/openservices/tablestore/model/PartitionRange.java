package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class PartitionRange {

    private PrimaryKeyValue begin; // The starting value of the range

    private PrimaryKeyValue end; // The end value of the range.

    /**
     * <p>
     * Constructs a new instance.
     * Represents a range that is closed on the left and open on the right.
     * </p>
     * <p>
     * Begin must be less than or equal to end. Integer types are compared by numerical value; character types are compared in lexicographical order.
     * </p>
     *
     * @param begin The start value of the range.
     * @param end   The end value of the range.
     */
    public PartitionRange(PrimaryKeyValue begin, PrimaryKeyValue end) {
        Preconditions.checkNotNull(begin, "The begin key of partition range should not be null.");
        Preconditions.checkNotNull(end, "The end key of partition range should not be null.");
        Preconditions.checkArgument(!begin.isInfMax() && !begin.isInfMin(),
                "The value of begin can't be INF_MIN or INF_MAX.");
        Preconditions.checkArgument(!end.isInfMax() && !end.isInfMin(),
                "The value of end can't be INF_MIN or INF_MAX.");

        if (!begin.getType().equals(end.getType())) {
            throw new IllegalArgumentException("The value type of begin and end must be the same.");
        }

        this.begin = begin;
        this.end = end;
    }


    /**
     * Returns the start value of the range.
     *
     * @return The start value of the range.
     */
    public PrimaryKeyValue getBegin() {
        return begin;
    }

    /**
     * Returns the end value of the range.
     *
     * @return The end value of the range.
     */
    public PrimaryKeyValue getEnd() {
        return end;
    }
}
