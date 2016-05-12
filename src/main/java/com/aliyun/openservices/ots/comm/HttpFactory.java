/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.comm;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.NTCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;

import com.aliyun.openservices.ots.ClientConfiguration;
import com.aliyun.openservices.ots.ClientException;

class HttpFactory {

    public CloseableHttpAsyncClient createHttpAsyncClient(
            ClientConfiguration config, PoolingNHttpClientConnectionManager cm) {
        HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClients.custom();
        httpClientBuilder.setConnectionManager(cm);
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(config.getConnectionTimeoutInMillisecond())
                .setSocketTimeout(config.getSocketTimeoutInMillisecond()).build();
        httpClientBuilder.setDefaultRequestConfig(requestConfig);
        httpClientBuilder.setUserAgent(config.getUserAgent());
        httpClientBuilder.disableCookieManagement();
        String proxyHost = config.getProxyHost();
        int proxyPort = config.getProxyPort();
        if (proxyHost != null) {
            if (proxyPort <= 0) {
                throw new ClientException("The proxy port is invalid. Please check your configuration.");
            }
            HttpHost proxy = new HttpHost(proxyHost, proxyPort);
            httpClientBuilder.setProxy(proxy);
            String proxyUsername = config.getProxyUsername();
            String proxyPassword = config.getProxyPassword();
            if (proxyUsername != null && proxyPassword != null) {
                String proxyDomain = config.getProxyDomain();
                String proxyWorkstation = config.getProxyWorkstation();
                CredentialsProvider credsProvider = new BasicCredentialsProvider();
                credsProvider.setCredentials(
                        new AuthScope(proxyHost, proxyPort), new NTCredentials(
                                proxyUsername, proxyPassword, proxyWorkstation,
                                proxyDomain));
                httpClientBuilder.setDefaultCredentialsProvider(credsProvider);
            }
        }
        return httpClientBuilder.build();
    }

}
