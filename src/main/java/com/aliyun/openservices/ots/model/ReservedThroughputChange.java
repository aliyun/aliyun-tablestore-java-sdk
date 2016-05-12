/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public class ReservedThroughputChange {
    /**
     * 新的CapacityUnit设置。
     */
    private CapacityUnit capacityUnit;
    
    /**
     * 标示用户是否更改了读CapacityUnit。
     */
    private boolean isReadSet;
    
    /**
     * 标示用户是否更改了写CapacityUnit。
     */
    private boolean isWriteSet;
    
    public ReservedThroughputChange() {
        this.isReadSet = false;
        this.isWriteSet = false;
        capacityUnit = new CapacityUnit();
    }
    
    /**
     * 设置读CapacityUnit。
     * @param value 读CapacityUnit。
     */
    public void setReadCapacityUnit(int value) {
        isReadSet = true;
        capacityUnit.setReadCapacityUnit(value);
    }
    
    /**
     * 设置写CapacityUnit。
     * @param value 写CapacityUnit。
     */
    public void setWriteCapacityUnit(int value) {
        isWriteSet = true;
        capacityUnit.setWriteCapacityUnit(value);
    }
    
    /**
     * 查看是否设置过读CapacityUnit。
     * @return 是否设置过读CapacityUnit。
     */
    public boolean isReadSet() {
        return isReadSet;
    }
    
    /**
     * 查看是否设置过写CapacityUnit。
     * @return 是否设置过写CapacityUnit。
     */
    public boolean isWriteSet() {
        return isWriteSet;
    }
    
    /**
     * 获取设置的读CapacityUnit。
     * @return 若设置了值，则返回设置的值，否则返回0。
     */
    public int getReadCapacityUnit() {
        if (isReadSet) {
            return capacityUnit.getReadCapacityUnit();
        }
        return 0;
    }
    
    /**
     * 获取设置的写CapacityUnit。
     * @return 若设置了值，则返回设置的值，否则返回0。
     */
    public int getWriteCapacityUnit() {
        if (isWriteSet) {
            return capacityUnit.getWriteCapacityUnit();
        }
        return 0;
    }
    
    /**
     * 清除设置的读CapacityUnit。
     */
    public void clearReadCapacityUnit() {
        isReadSet = false;
    }
    
    /**
     * 清除设置的写CapacityUnit。
     */
    public void clearWriteCapacityUnit() {
        isWriteSet = false;
    }
}
