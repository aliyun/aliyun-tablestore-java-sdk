package com.alicloud.openservices.tablestore.model;

public interface Measurable {
    /**
     * 获取序列化后的数据大小
     *
     * @return 序列化后的数据大小
     */
    int getDataSize();
}
