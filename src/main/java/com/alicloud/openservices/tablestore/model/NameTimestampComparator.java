package com.alicloud.openservices.tablestore.model;

import java.util.Comparator;

public class NameTimestampComparator implements Comparator<Column> {
    /**
     * 比较两个属性列的名称和时间戳。
     * <p>拿此比较器排序后的结果为先按名称升序，后按时间戳降序。</p>
     * <p>若比较的两个Column没有设置过时间戳，则无法进行比较。</p>
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
