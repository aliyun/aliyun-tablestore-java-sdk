/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

public interface OTSFutureCallback<T> {

    void completed(T result);

    void failed(Exception ex);

}