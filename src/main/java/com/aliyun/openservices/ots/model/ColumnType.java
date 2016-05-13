package com.aliyun.openservices.ots.model;

import java.util.HashMap;
import java.util.Map;

/**
 * 表示表（Table）中列的数据类型。
 *
 */
public enum ColumnType {
    /**
     * 字符串型。
     */
    STRING,

    /**
     * 64位带符号的整型。
     */
    INTEGER,

    /**
     * 布尔型。
     */
    BOOLEAN,

    /**
     * 64位浮点型。
     */
    DOUBLE,
    
    /**
     * 二进制数据。
     */
    BINARY;

    private static final Map<String, ColumnType> strToEnum = new HashMap<String, ColumnType>();

    static { // Initialize the map
        for(ColumnType t : values()){
            strToEnum.put(t.toString(), t);
        }
    }

    // package-private only.
    static ColumnType fromString(String value){
        if (value == null){
            throw new NullPointerException();
        }
        return strToEnum.get(value);
    }
}
