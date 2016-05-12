/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTSException;

/*
 * 关于OTSFuture接口的使用，可以参考java.util.concurrent.Future。
 * OTSFuture没有提供类似Future.cancel()的cancel方法，
 * 因为与OTS的连接建立之后，实际上已不能取消本次请求。
 * 
 */
public interface OTSFuture<V> {

    public boolean isDone();

    public V get() throws ClientException, OTSException;

    public V get(long timeout, TimeUnit unit) throws ClientException,
            OTSException, TimeoutException;
}
