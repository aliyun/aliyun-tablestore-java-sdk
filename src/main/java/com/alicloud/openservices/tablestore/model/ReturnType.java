package com.alicloud.openservices.tablestore.model;

/**
 * 表示操作（PUT，UPDATE，DELETE）的返回结果中是否附带PK值，对于PK递增列，应该设置返回PK
 */
public enum ReturnType {
    /**
     * 不返回任何行数据。
     */
	RT_NONE,

    /**
     * 返回PK列的数据。
     */
	RT_PK,

    /**
     * 返回修改列的数据(如原子加的结果返回）。
     */
    RT_AFTER_MODIFY,
}
