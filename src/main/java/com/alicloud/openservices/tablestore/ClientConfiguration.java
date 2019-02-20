package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.DefaultRetryStrategy;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

/**
 * 访问阿里云服务的客户端配置。
 */
public class ClientConfiguration {
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    private int ioThreadCount = AVAILABLE_PROCESSORS;
    private int maxConnections = 300;
    private int socketTimeoutInMillisecond = 30 * 1000;
    private int connectionTimeoutInMillisecond = 30 * 1000;
    private int retryThreadCount = 1;

    private boolean enableRequestCompression = false;
    private boolean enableResponseCompression = false;
    private boolean enableResponseValidation = true;
    private boolean enableResponseContentMD5Checking = false;
    private RetryStrategy retryStrategy = new DefaultRetryStrategy();
    private int timeThresholdOfServerTracer = 500;
    private int timeThresholdOfTraceLogger = 1000;

    private String proxyHost;
    private int proxyPort;
    private String proxyUsername;
    private String proxyPassword;
    private String proxyDomain;
    private String proxyWorkstation;

    private long syncClientWaitFutureTimeoutInMillis = 60 * 1000;

    /**
     * 构造新实例。
     */
    public ClientConfiguration() {
    }


    /**
     * 设置是否需要对请求数据进行压缩
     * @param enableRequestCompression
     *          是否需要压缩(true/false)
     */
    public void setEnableRequestCompression(boolean enableRequestCompression) {
        this.enableRequestCompression = enableRequestCompression;
    }    

    /**
     * 返回是否需要对请求数据进行压缩
     * @return 是否需要对request进行压缩
     */
    public boolean isEnableRequestCompression() {
        return enableRequestCompression;
    }

    /**
     * 是否需要告知TableStore对返回的响应内容进行压缩
     * @param enableResponseCompression
     *          是否告知TableStore对返回的响应内容进行压缩
     */
    public void setEnableResponseCompression(boolean enableResponseCompression) {
        this.enableResponseCompression = enableResponseCompression;
    }
    
    /**
     * 返回是否需要告知TableStore对返回的响应内容进行压缩
     * @return
     *      是否需要告知TableStore对返回的响应内容进行压缩
     */ 
    public boolean isEnableResponseCompression() {
        return enableResponseCompression;
    }
    
    /**
     * 返回是否需要对响应进行验证
     * @return
     *      是否需要对响应进行验证
     */
    public boolean isEnableResponseValidation() {
        return enableResponseValidation;
    }
    
    /**
     * 是否需要对响应进行验证， 如果需要验证， 
     * Client会验证头信息完整性、结果是否过期、授权信息是否正确
     * @param enableResponseValidation
     *          是否需要对响应进行验证
     */
    public void setEnableResponseValidation(boolean enableResponseValidation) {
        this.enableResponseValidation = enableResponseValidation;
    }

    /**
     * 返回HttpAsyncClient的IOReactor的线程数。
     *
     * @return IOReactor的线程数
     */
    public int getIoThreadCount() {
        return ioThreadCount;
    }

    /**
     * 设置HttpAsyncClient的IOReactor的线程数(因为采用的是异步IO，所以不需要配置大量线程，每个线程都能提供大量并发)。
     *
     * @param ioThreadCount IOReactor的线程数
     */
    public void setIoThreadCount(int ioThreadCount) {
        Preconditions.checkArgument(ioThreadCount > 0, "The io thread count must be greater than 0.");
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
     * @param maxConnections 最大HTTP连接数。
     */
    public void setMaxConnections(int maxConnections) {
        Preconditions.checkArgument(maxConnections > 0, "The max connection must be greater than 0.");
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
     * @param socketTimeoutInMillisecond 通过打开的连接传输数据的超时时间（单位：毫秒）。
     */
    public void setSocketTimeoutInMillisecond(int socketTimeoutInMillisecond) {
        Preconditions.checkArgument(socketTimeoutInMillisecond > 0, "The socket timeout must be greater than 0.");
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
     * @param connectionTimeoutInMillisecond 建立连接的超时时间（单位：毫秒）。
     */
    public void setConnectionTimeoutInMillisecond(int connectionTimeoutInMillisecond) {
        Preconditions.checkArgument(connectionTimeoutInMillisecond > 0, "The connection timeout must be greater than 0.");
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
     *
     * @param retryThreadCount 用于执行错误重试的线程池的线程的个数。
     */
    public void setRetryThreadCount(int retryThreadCount) {
        Preconditions.checkArgument(retryThreadCount > 0, "The retry thread count must be greater than 0.");
        this.retryThreadCount = retryThreadCount;
    }

    /**
     * 返回是否需要对响应内容做MD5校验
     *
     * @return 是否需要对MD5内容做MD5校验
     */
    public boolean isEnableResponseContentMD5Checking() {
        return enableResponseContentMD5Checking;
    }

    /**
     * 是否需要对响应的内容做MD5校验， 如果需要校验，
     * Client会计算响应数据的MD5值并与返回的响应头中的x-ots-contentmd5头的值进行比对
     *
     * @param enableResponseContentMD5Checking 是否需要校验响应数据MD5
     */
    public void setEnableResponseContentMD5Checking(
            boolean enableResponseContentMD5Checking) {
        this.enableResponseContentMD5Checking = enableResponseContentMD5Checking;
    }

    /**
     * 返回TableStore的请求重试策略
     *
     * @return 请求重试策略
     */
    public RetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    /**
     * 设置TableStore的请求重试策略
     *
     * @param retryStrategy TableStore的请求重试策略
     */
    public void setRetryStrategy(RetryStrategy retryStrategy) {
        Preconditions.checkArgument(retryStrategy != null, "The retry strategy should not be null.");
        this.retryStrategy = retryStrategy;
    }

    /**
     * 返回当前设置的时间阈值(单位：毫秒)。
     * 当一个请求的总执行时间(包含重试占用的时间)超过该阈值时，SDK会记录一条WARN级别的日志。
     * 该功能依赖于日志相关的配置。
     *
     * @return 时间阈值
     */
    public int getTimeThresholdOfTraceLogger() {
        return timeThresholdOfTraceLogger;
    }

    /**
     * 设置一个时间阈值(单位：毫秒)。
     * 当一个请求的总执行时间(包含重试占用的时间)超过该阈值时，SDK会记录一条WARN级别的日志。
     * 该功能依赖于日志相关的配置。
     *
     * @param timeThresholdOfTraceLogger trace logger
     */
    public void setTimeThresholdOfTraceLogger(int timeThresholdOfTraceLogger) {
        Preconditions.checkArgument(timeThresholdOfTraceLogger > 0, "The time threshold of trace logger must be greater than 0.");
        this.timeThresholdOfTraceLogger = timeThresholdOfTraceLogger;
    }

    /**
     * 返回当前设置的服务端Tracer时间阈值(单位：毫秒)。
     * 当一个请求在服务端的执行时间超过该阈值时，SDK会收到服务端的tracer信息并记录。
     * 该功能依赖于服务端相关配置。
     *
     * @return 时间阈值
     */
    public int getTimeThresholdOfServerTracer() {
        return timeThresholdOfServerTracer;
    }

    /**
     * 设置服务端Tracer的时间阈值(单位：毫秒)。
     * 当一个请求在服务端的执行时间超过该阈值时，SDK会收到服务端的tracer信息并记录。
     * 该功能依赖于服务端相关配置
     *
     * @param timeThresholdOfServerTracer trace logger
     */
    public void setTimeThresholdOfServerTracer(int timeThresholdOfServerTracer) {
        Preconditions.checkArgument(timeThresholdOfServerTracer > 0, "The time threshold of server tracer must be greater than 0.");
        this.timeThresholdOfServerTracer = timeThresholdOfServerTracer;
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
     * 获取同步Client内等待异步调用返回的最大超时时间。
     * @return
     */
    public long getSyncClientWaitFutureTimeoutInMillis() {
        return syncClientWaitFutureTimeoutInMillis;
    }

    /**
     * 设置同步Client内等待异步调用返回的最大超时时间。
     * @param syncClientWaitFutureTimeoutInMillis
     */
    public void setSyncClientWaitFutureTimeoutInMillis(long syncClientWaitFutureTimeoutInMillis) {
        this.syncClientWaitFutureTimeoutInMillis = syncClientWaitFutureTimeoutInMillis;
    }
}
