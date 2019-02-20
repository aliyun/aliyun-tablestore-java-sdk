package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

public class PartitionRange {

    private PrimaryKeyValue begin; // 范围的起始值

    private PrimaryKeyValue end; // 范围的结束值。

    /**
     * <p>
     * 构造一个新的实例。
     * 表示左闭右开的范围。
     * </p>
     * <p>
     * begin必须小于或等于end。整型按数字大小比较；字符型按字典顺序比较；
     * </p>
     *
     * @param begin 范围的起始值。
     * @param end   范围的结束值。
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
     * 返回范围的起始值。
     *
     * @return 范围的起始值。
     */
    public PrimaryKeyValue getBegin() {
        return begin;
    }

    /**
     * 返回范围的终止值。
     *
     * @return 范围的终止值。
     */
    public PrimaryKeyValue getEnd() {
        return end;
    }
}
