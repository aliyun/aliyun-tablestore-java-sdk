package com.alicloud.openservices.tablestore.model.search.sort;

/**
 * 选取一个Field的哪个值进行排序
 * <p>大多数情况下，一个字段只有一个值，所以这项无意义，默认为 MIN</p>
 * <p>当一个字段是一个集合（Array）时候，可以设置用该集合中的某一个值作为排序的依据</p>
 */
public enum SortMode {
    MIN,
    MAX,
    AVG
}
