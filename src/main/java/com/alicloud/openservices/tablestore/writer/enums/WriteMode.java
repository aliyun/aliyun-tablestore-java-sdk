package com.alicloud.openservices.tablestore.writer.enums;


public enum WriteMode {
    /**
     * 串行写：
     * 不同筒间并发，同一个筒内串行请求
     * */
    SEQUENTIAL,
    /**
     * 并行写
     * 不同筒间并发，同一个筒内也会并行请求
     * */
    PARALLEL
}
