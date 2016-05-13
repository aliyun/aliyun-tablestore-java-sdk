package com.aliyun.openservices.ots;

import com.aliyun.openservices.ots.utils.VersionInfoUtils;

/**
 * 访问阿里云服务的客户端配置。
 * 
 *
 */
public class ClientConfiguration {
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime()
            .availableProcessors();
    private static final String DEFAULT_USER_AGENT = VersionInfoUtils
            .getDefaultUserAgent();

    private String userAgent = DEFAULT_USER_AGENT;
    private String proxyHost;
    private int proxyPort;
    private String proxyUsername;
    private String proxyPassword;
    private String proxyDomain;
    private String proxyWorkstation;
    private int ioThreadCount = AVAILABLE_PROCESSORS;
    private int maxConnections = 300;
    private int socketTimeoutInMillisecond = 15 * 1000;
    private int connectionTimeoutInMillisecond = 15 * 1000;
    private int retryThreadCount = 1;
    
    
    /**
     * 构造新实例。
     */
    public ClientConfiguration() {
    }

    /**
     * 构造用户代理。
     * 
     * @return 用户代理。
     */
    public String getUserAgent() {
        return userAgent;
    }

    /**
     * 设置用户代理。
     * 
     * @param userAgent
     *            用户代理。
     */
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    /**
     * 返回代理服务器主机地址。
     * 
     * @return 代理服务器主机地址。
     */
    public String getProxyHost() {
        return proxyHost;
    }

    /**
     * 设置代理服务器主机地址。
     * 
     * @param proxyHost
     *            代理服务器主机地址。
     */
    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    /**
     * 返回代理服务器端口。
     * 
     * @return 代理服务器端口。
     */
    public int getProxyPort() {
        return proxyPort;
    }

    /**
     * 设置代理服务器端口。
     * 
     * @param proxyPort
     *            代理服务器端口。
     */
    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    /**
     * 返回代理服务器验证的用户名。
     * 
     * @return 用户名。
     */
    public String getProxyUsername() {
        return proxyUsername;
    }

    /**
     * 设置代理服务器验证的用户名。
     * 
     * @param proxyUsername
     *            用户名。
     */
    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    /**
     * 返回代理服务器验证的密码。
     * 
     * @return 密码。
     */
    public String getProxyPassword() {
        return proxyPassword;
    }

    /**
     * 设置代理服务器验证的密码。
     * 
     * @param proxyPassword
     *            密码。
     */
    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    /**
     * 返回访问NTLM验证的代理服务器的Windows域名（可选）。
     * 
     * @return 域名。
     */
    public String getProxyDomain() {
        return proxyDomain;
    }

    /**
     * 设置访问NTLM验证的代理服务器的Windows域名（可选）。
     * 
     * @param proxyDomain
     *            域名。
     */
    public void setProxyDomain(String proxyDomain) {
        this.proxyDomain = proxyDomain;
    }

    /**
     * 返回NTLM代理服务器的Windows工作站名称。
     * 
     * @return NTLM代理服务器的Windows工作站名称。
     */
    public String getProxyWorkstation() {
        return proxyWorkstation;
    }

    /**
     * 设置NTLM代理服务器的Windows工作站名称。 （可选，如果代理服务器非NTLM，不需要设置该参数）。
     * 
     * @param proxyWorkstation
     *            NTLM代理服务器的Windows工作站名称。
     */
    public void setProxyWorkstation(String proxyWorkstation) {
        this.proxyWorkstation = proxyWorkstation;
    }

    /**
     * 返回HttpAsyncClient的IOReactor的线程数。
     * @return IOReactor的线程数
     */
    public int getIoThreadCount() {
        return ioThreadCount;
    }

    /**
     * 设置HttpAsyncClient的IOReactor的线程数(因为采用的是异步IO，所以不需要配置大量线程，每个线程都能提供大量并发)。
     * @param ioThreadCount
     *              IOReactor的线程数
     */
    public void setIoThreadCount(int ioThreadCount) {
        this.ioThreadCount = ioThreadCount;
    }

    /**
     * 返回允许打开的最大HTTP连接数。
     * 
     * @return 最大HTTP连接数。
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * 设置允许打开的最大HTTP连接数(连接数影响并发度，需要根据QPS、单个请求的时间、机器配置等因素来确定一个合理的值)。
     * 
     * @param maxConnections
     *            最大HTTP连接数。
     */
    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    /**
     * 返回通过打开的连接传输数据的超时时间（单位：毫秒）。 0表示无限等待（但不推荐使用）。受系统超时时间的限制。
     * 
     * @return 通过打开的连接传输数据的超时时间（单位：毫秒）。
     */
    public int getSocketTimeoutInMillisecond() {
        return socketTimeoutInMillisecond;
    }

    /**
     * 设置通过打开的连接传输数据的超时时间（单位：毫秒）。 0表示无限等待（但不推荐使用）。受系统超时时间的限制。
     * 
     * @param socketTimeoutInMillisecond
     *            通过打开的连接传输数据的超时时间（单位：毫秒）。
     */
    public void setSocketTimeoutInMillisecond(int socketTimeoutInMillisecond) {
        this.socketTimeoutInMillisecond = socketTimeoutInMillisecond;
    }

    /**
     * 返回建立连接的超时时间（单位：毫秒）。0表示无限等待（但不推荐使用）。受系统超时时间的限制。
     * 
     * @return 建立连接的超时时间（单位：毫秒）。
     */
    public int getConnectionTimeoutInMillisecond() {
        return connectionTimeoutInMillisecond;
    }

    /**
     * 设置建立连接的超时时间（单位：毫秒）。0表示无限等待（但不推荐使用）。受系统超时时间的限制。
     * 
     * @param connectionTimeoutInMillisecond
     *            建立连接的超时时间（单位：毫秒）。
     */
    public void setConnectionTimeoutInMillisecond(int connectionTimeoutInMillisecond) {
        this.connectionTimeoutInMillisecond = connectionTimeoutInMillisecond;
    }

    /**
     * 返回用于执行错误重试的线程池的线程的个数。该线程池为一个ScheduledExecutorService。
     * 
     * @return 用于执行错误重试的线程池的线程的个数。
     */
    public int getRetryThreadCount() {
        return retryThreadCount;
    }

    /**
     * 设置用于执行错误重试的线程池的线程的个数。该线程池为一个ScheduledExecutorService。
     * @param retryThreadCount
     *            用于执行错误重试的线程池的线程的个数。
     */
    public void setRetryThreadCount(int retryThreadCount) {
        this.retryThreadCount = retryThreadCount;
    }

}
