/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

/**
 * 表示表（Table）的CapacityUnit设置。
 */
public class CapacityUnit {
    /**
     * 表的读CapacityUnit设置。
     */
    private int readCapacityUnit;
    
    /**
     * 表的写CapacityUnit设置。
     */
    private int writeCapacityUnit;
    
    /**
     * 默认构造函数，读CapacityUnit和写CapacityUnit的值都将初始化为0。
     */
    public CapacityUnit() {
        this(0, 0);
    }
    
    /**
     * 构造CapacityUnit对象，并指定表的读CapacityUnit和写CapacityUnit配置。
     * @param readCapacityUnit 表的读CapacityUnit，其值必须大于等于0。
     * @param writeCapacityUnit 写CapacityUnit，其值必须大于等于0。
     * @throws IllegalArgumentException 
     *              若读CapacityUnit或写CapacityUnit的值负数。
     */
    public CapacityUnit(int readCapacityUnit, int writeCapacityUnit) {
        setReadCapacityUnit(readCapacityUnit);
        setWriteCapacityUnit(writeCapacityUnit);
    }
    
    /**
     * 获取读CapacityUnit设置。
     * @return 读CapacityUnit。
     */
    public int getReadCapacityUnit() {
        return readCapacityUnit;
    }

    /**
     * 设置创建表时初始读CapacityUnit的值，设置的值必须为非负数。
     * @param readCapacityUnit 读CapacityUnit的配置
     * @throws IllegalArgumentException 
     *              若读CapacityUnit的值为负数。
     */
    public void setReadCapacityUnit(int readCapacityUnit) {
        if (readCapacityUnit < 0) {
            throw new IllegalArgumentException("The read capacity unit must be positive.");
        }
        this.readCapacityUnit = readCapacityUnit;
    }

    /**
     * 获取写CapacityUnit设置。
     * @return 写CapacityUnit。
     */
    public int getWriteCapacityUnit() {
        return writeCapacityUnit;
    }

    /**
     * 设置创建表时初始写CapacityUnit的值，设置的值必须为非负数。
     * @param readCapacityUnit 写CapacityUnit的配置
     * @throws IllegalArgumentException 
     *              若写CapacityUnit的值为负数。
     */
    public void setWriteCapacityUnit(int writeCapacityUnit) {
        if (readCapacityUnit < 0) {
            throw new IllegalArgumentException("The write capacity unit must be positive.");
        }
        this.writeCapacityUnit = writeCapacityUnit;
    }
}
