package com.alicloud.openservices.tablestore.model;

public interface Measurable {
    /**
     * Get the size of the serialized data
     *
     * @return the size of the serialized data
     */
    int getDataSize();
}
