/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 表示主键（PrimaryKey）的数据类型。
 *
 */
public enum PrimaryKeyType {
    /**
     * 字符串型。
     */
    STRING,

    /**
     * 64位整型。
     */
    INTEGER,

    /**
     * 字节数组。
     */
    BINARY;

    private static final Map<String, PrimaryKeyType> strToEnum = 
            new HashMap<String, PrimaryKeyType>();
    static { // Initialize the map
        for(PrimaryKeyType t : values()){
            strToEnum.put(t.toString(), t);
        }
    }

    // Package-private only.
    static PrimaryKeyType fromString(String value){
        if (value == null){
            throw new NullPointerException();
        }
        return strToEnum.get(value);
    }
}
