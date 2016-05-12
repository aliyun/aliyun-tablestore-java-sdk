/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 *
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots;

import com.aliyun.openservices.ots.ServiceException;


/**
 * 该异常在对开放结构化数据服务（Open Table Service）访问失败时抛出。
 */
public class OTSException extends ServiceException {

    private static final long serialVersionUID = 781283289032342L;
    private int httpStatus = 200;

    /**
     * 构造函数。
     */
    public OTSException() {
        super();
    }

    /**
     * 构造函数。
     *
     * @param message 错误消息。
     */
    public OTSException(String message) {
        super(message);
    }

    /**
     * 构造函数。
     *
     * @param message 错误消息。
     * @param cause   错误原因。
     */
    public OTSException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * 构造函数。
     *
     * @param message    错误消息。
     * @param cause      错误原因。
     * @param errorCode  错误代码。
     * @param requestId  Request标识。
     * @param httpStatus Http状态码
     */
    public OTSException(String message, Throwable cause,
                        String errorCode,
                        String requestId, int httpStatus) {
        super(message, cause, errorCode, requestId, "");
        this.httpStatus = httpStatus;
    }

    public OTSException(String message, String code, String requestId) {
        this(message, null, code, requestId, 0);
    }

    /**
     * 返回Http状态码
     *
     * @return Http状态码
     */
    public int getHttpStatus() {
        return httpStatus;
    }

}