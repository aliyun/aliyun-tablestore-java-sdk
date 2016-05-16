package com.aliyun.openservices.ots.comm;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.ServiceException;

/**
 * 对返回结果进行处理。
 *
 */
public interface ResponseHandler {

    /**
     * 处理返回的结果
     * @param responseData 服务器返回的数据
     * @throws ServiceException
     * @throws ClientException
     */
    public void handle(ResponseMessage responseData)
            throws ServiceException, ClientException;
}
