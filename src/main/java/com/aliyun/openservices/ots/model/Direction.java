package com.aliyun.openservices.ots.model;

public enum Direction {
    /**
     * 正序读。
     */
    FORWARD("FORWARD"),
    /**
     * 反序读。
     */
    BACKWARD("BACKWARD");
    
    private String name;
    
    private Direction(String name) {
        this.name = name;
    }
    
    @Override
    public String toString() {
        return name;
    }
}
