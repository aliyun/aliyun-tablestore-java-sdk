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
