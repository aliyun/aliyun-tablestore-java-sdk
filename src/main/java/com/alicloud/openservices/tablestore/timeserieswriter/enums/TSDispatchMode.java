package com.alicloud.openservices.tablestore.timeserieswriter.enums;


public enum TSDispatchMode {
    /**
     * 循环遍历分桶派发
     */
    ROUND_ROBIN,

    /**
     * 哈希完整主键分桶派发
     * */
    HASH_PRIMARY_KEY,
}
