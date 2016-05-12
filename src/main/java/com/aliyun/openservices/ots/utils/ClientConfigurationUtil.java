/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.utils;

import com.aliyun.openservices.ots.ClientConfiguration;

public class ClientConfigurationUtil {
    public static void copyConfig(ClientConfiguration source, ClientConfiguration target) {
        target.setIoThreadCount(source.getIoThreadCount());
        target.setConnectionTimeoutInMillisecond(source.getConnectionTimeoutInMillisecond());
        target.setMaxConnections(source.getMaxConnections());
        target.setProxyDomain(source.getProxyDomain());
        target.setProxyHost(source.getProxyHost());
        target.setProxyPassword(source.getProxyPassword());
        target.setProxyPort(source.getProxyPort());
        target.setProxyUsername(source.getProxyUsername());
        target.setProxyWorkstation(source.getProxyWorkstation());
        target.setSocketTimeoutInMillisecond(source.getSocketTimeoutInMillisecond());
        target.setUserAgent(source.getUserAgent());
        target.setRetryThreadCount(source.getRetryThreadCount());
    }
}
