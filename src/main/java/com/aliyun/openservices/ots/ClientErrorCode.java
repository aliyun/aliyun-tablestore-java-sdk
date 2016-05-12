/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots;

public interface ClientErrorCode {
    
    /**
     * 未知错误
     */
    static final String UNKNOWN = "Unknown"; 
    
    /**
     * 远程服务连接超时
     */
    static final String CONNECTION_TIMEOUT = "ConnectionTimeout";
    
    /**
     * 远程服务socket读写超时
     */
    static final String SOCKET_TIMEOUT = "SocketTimeout";
    
    /**
     * 返回结果无法解析
     */
    static final String INVALID_RESPONSE = "InvalidResponse";
}
