package com.alicloud.openservices.tablestore.model.search.query;

/**
 * 一个字段多个值的情况下，采用哪个值来进行排序。
 * <p>举例：有一个小学生学生状态监测系统，其中存了小学生的身高，但是小学生身高一直在长，所以“身高”这个字段，采用了array的方式。然后我们查询的时候，想根据身高进行排序，就可以设置ScoreMode为MAX,这样就能得到最近的一次身高</p>
 */
public enum ScoreMode {
    None,
    Avg,
    Max,
    Min,
    Total,
}
