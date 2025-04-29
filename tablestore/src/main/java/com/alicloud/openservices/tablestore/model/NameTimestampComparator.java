package com.alicloud.openservices.tablestore.model;

import java.util.Comparator;

public class NameTimestampComparator implements Comparator<Column> {
    /**
     * Compare the names and timestamps of two property columns.
     * <p>The result after sorting with this comparator will be ordered by name in ascending order first, then by timestamp in descending order.</p>
     * <p>If the two Columns being compared have never had their timestamp set, they cannot be compared.</p>
     *
     * @param c1
     * @param c2
     * @return Result
     */
    @Override
    public int compare(Column c1, Column c2) {
        int ret = c1.getName().compareTo(c2.getName());
        if (ret != 0) {
            return ret;
        }
        long t1 = Long.MAX_VALUE;
        if (c1.hasSetTimestamp()) {
             t1 = c1.getTimestamp();
        }
        long t2 = Long.MAX_VALUE;
        if (c2.hasSetTimestamp()) {
            t2 = c2.getTimestamp();
        }
        return t1 == t2 ? 0 : (t1 < t2 ? 1 : -1);
    }
}
