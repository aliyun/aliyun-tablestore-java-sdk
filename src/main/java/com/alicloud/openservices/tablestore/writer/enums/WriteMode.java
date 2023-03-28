package com.alicloud.openservices.tablestore.writer.enums;


public enum WriteMode {
    /**
     * 串行写：
     * 不同桶间并发，同一个桶内串行请求
     * */
    SEQUENTIAL,
    /**
     * 并行写
     * 不同桶间并发，同一个桶内也会并行请求
     * */
    PARALLEL
}
