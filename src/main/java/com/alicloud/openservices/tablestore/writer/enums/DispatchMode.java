package com.alicloud.openservices.tablestore.writer.enums;


public enum DispatchMode {
    /**
     * 循环遍历分桶派发
     */
    ROUND_ROBIN,
    /**
     * 哈希分区键分桶派发
     * */
    HASH_PARTITION_KEY,
    /**
     * 哈希完整主键分桶派发
     * */
    HASH_PRIMARY_KEY,
}
