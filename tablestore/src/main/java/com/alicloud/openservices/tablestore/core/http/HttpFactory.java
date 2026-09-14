package com.alicloud.openservices.tablestore.core.http;

import com.aliyun.ots.thirdparty.org.apache.http.HttpHost;
import com.aliyun.ots.thirdparty.org.apache.http.client.CredentialsProvider;
import com.aliyun.ots.thirdparty.org.apache.http.impl.client.BasicCredentialsProvider;
import com.aliyun.ots.thirdparty.org.apache.http.auth.AuthScope;
import com.aliyun.ots.thirdparty.org.apache.http.auth.NTCredentials;
import com.aliyun.ots.thirdparty.org.apache.http.auth.UsernamePasswordCredentials;
import com.aliyun.ots.thirdparty.org.apache.http.client.config.RequestConfig;
import com.aliyun.ots.thirdparty.org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import com.aliyun.ots.thirdparty.org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import com.aliyun.ots.thirdparty.org.apache.http.impl.nio.client.HttpAsyncClients;
import com.aliyun.ots.thirdparty.org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;

import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.ClientException;

class HttpFactory {
    public static CloseableHttpAsyncClient createHttpAsyncClient(
            ClientConfiguration config, PoolingNHttpClientConnectionManager cm) {
        HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClients.custom();
        httpClientBuilder.setConnectionManager(cm);
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom()
                .setConnectTimeout(config.getConnectionTimeoutInMillisecond())
                .setSocketTimeout(config.getSocketTimeoutInMillisecond());
        if (config.getConnectionRequestTimeoutInMillisecond() > 0) {
            requestConfigBuilder.setConnectionRequestTimeout(config.getConnectionRequestTimeoutInMillisecond());
        }
        httpClientBuilder.setDefaultRequestConfig(requestConfigBuilder.build());
        httpClientBuilder.setUserAgent(Constants.USER_AGENT);
        httpClientBuilder.disableCookieManagement();

        String proxyHost = config.getProxyHost();
        int proxyPort = config.getProxyPort();
        EnvironmentProxyRoutePlanner environmentRoutePlanner =
                createEnvironmentProxyRoutePlanner(config, System.getenv());
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
                    new AuthScope(proxyHost, proxyPort),
                    new NTCredentials(
                        proxyUsername, proxyPassword, proxyWorkstation, proxyDomain));
                httpClientBuilder.setDefaultCredentialsProvider(credsProvider);
            }
        } else {
            if (environmentRoutePlanner.hasProxy()) {
                httpClientBuilder.setRoutePlanner(environmentRoutePlanner);
                CredentialsProvider credsProvider = createEnvironmentProxyCredentials(environmentRoutePlanner);
                if (credsProvider != null) {
                    httpClientBuilder.setDefaultCredentialsProvider(credsProvider);
                }
            }
        }

        return httpClientBuilder.build();
    }

    static EnvironmentProxyRoutePlanner createEnvironmentProxyRoutePlanner(
            ClientConfiguration config, java.util.Map<String, String> environment) {
        return config.getProxyHost() == null ? new EnvironmentProxyRoutePlanner(environment) : null;
    }

    private static CredentialsProvider createEnvironmentProxyCredentials(
            EnvironmentProxyRoutePlanner routePlanner) {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        boolean hasCredentials = addCredentials(credentialsProvider, routePlanner.getHttpProxy());
        hasCredentials |= addCredentials(credentialsProvider, routePlanner.getHttpsProxy());
        return hasCredentials ? credentialsProvider : null;
    }

    private static boolean addCredentials(
            BasicCredentialsProvider credentialsProvider,
            EnvironmentProxyRoutePlanner.ProxySetting proxy) {
        if (proxy == null || proxy.getUsername() == null) {
            return false;
        }
        HttpHost host = proxy.getHost();
        credentialsProvider.setCredentials(
                new AuthScope(host.getHostName(), host.getPort()),
                new UsernamePasswordCredentials(proxy.getUsername(), proxy.getPassword()));
        return true;
    }
}
