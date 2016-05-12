/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.comm;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.ServiceException;

/**
 * 对即将发送的请求数据进行预处理
 *
 */
public interface RequestHandler {

    /**
     * 预处理需要发送的请求数据
     */
    public void handle(RequestMessage message)
            throws ServiceException, ClientException;
}
