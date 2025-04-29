package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.DefaultRetryStrategy;
import com.alicloud.openservices.tablestore.model.RetryStrategy;

/**
 * Client configuration for accessing the Alibaba Cloud service.
 */
public class ClientConfiguration {
    private static final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

    private int ioThreadCount = AVAILABLE_PROCESSORS;
    private int maxConnections = 300;
    private int socketTimeoutInMillisecond = 30 * 1000;
    private int connectionTimeoutInMillisecond = 30 * 1000;
    private int connectionRequestTimeoutInMillisecond = -1;
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

    private TimeseriesConfiguration timeseriesConfiguration;

    /**
     * Whether to enable link tracking. Default is not enabled.
     */
    private boolean enableRequestTracer = false;

    /**
     * DNS cache related configuration. Default is not enabled.
     */
    private boolean enableDnsCache = false;
    private int dnsCacheMaxSize = 100;
    private int dnsCacheExpireAfterWriteSec = 600;
    private int dnsCacheRefreshAfterWriteSec = 300;

    /**
     * Tracing system interface
     */
    private RequestTracer requestTracer;

    /**
     * Set the cache size and timeout for the SSL session;
     * If not configured, the JDK default values will be used:
     * You can check the default values via SSLContexts.createDefault().getSessionCacheSize() and SSLContexts.createDefault().getSessionTimeout().
     */
    private int sslSessionCacheSize = -1; // -1 means no configuration
    private int sslSessionTimeoutInSec = -1; // -1 means no configuration

    /**
     * Constructs a new instance.
     */
    public ClientConfiguration() {
    }


    /**
     * Set whether to enable compression for request data
     *
     * @param enableRequestCompression Whether to compress (true/false)
     */
    public void setEnableRequestCompression(boolean enableRequestCompression) {
        this.enableRequestCompression = enableRequestCompression;
    }

    /**
     * Returns whether the request data needs to be compressed.
     *
     * @return Whether the request needs to be compressed.
     */
    public boolean isEnableRequestCompression() {
        return enableRequestCompression;
    }

    /**
     * Whether to inform TableStore to compress the content of the returned response.
     *
     * @param enableResponseCompression whether to inform TableStore to compress the content of the returned response.
     */
    public void setEnableResponseCompression(boolean enableResponseCompression) {
        this.enableResponseCompression = enableResponseCompression;
    }

    /**
     * Returns whether to inform TableStore to compress the response content.
     *
     * @return whether to inform TableStore to compress the response content
     */
    public boolean isEnableResponseCompression() {
        return enableResponseCompression;
    }

    /**
     * Returns whether the response needs to be verified.
     *
     * @return Whether the response needs to be verified.
     */
    public boolean isEnableResponseValidation() {
        return enableResponseValidation;
    }

    /**
     * Whether to enable validation of the response. If enabled, 
     * the Client will validate the integrity of the headers, whether the results are expired, and the correctness of the authorization information.
     *
     * @param enableResponseValidation whether to enable validation of the response
     */
    public void setEnableResponseValidation(boolean enableResponseValidation) {
        this.enableResponseValidation = enableResponseValidation;
    }

    /**
     * Returns the number of threads for the IOReactor of the HttpAsyncClient.
     *
     * @return the number of threads for the IOReactor
     */
    public int getIoThreadCount() {
        return ioThreadCount;
    }

    /**
     * Set the number of threads for the IOReactor of the HttpAsyncClient (since asynchronous IO is used, a large number of threads does not need to be configured, and each thread can provide high concurrency).
     *
     * @param ioThreadCount Number of threads for the IOReactor
     */
    public void setIoThreadCount(int ioThreadCount) {
        Preconditions.checkArgument(ioThreadCount > 0, "The io thread count must be greater than 0.");
        this.ioThreadCount = ioThreadCount;
    }

    /**
     * Returns the maximum number of allowed open HTTP connections.
     *
     * @return The maximum number of HTTP connections.
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Set the maximum number of HTTP connections allowed to be opened (the number of connections affects concurrency and should be determined by factors such as QPS, the time for a single request, machine configuration, etc., to set a reasonable value).
     *
     * @param maxConnections The maximum number of HTTP connections.
     */
    public void setMaxConnections(int maxConnections) {
        Preconditions.checkArgument(maxConnections > 0, "The max connection must be greater than 0.");
        this.maxConnections = maxConnections;
    }

    /**
     * Returns the timeout for transmitting data via the opened connection (unit: milliseconds). 
     * 0 means infinite waiting (but not recommended). Subject to the system's timeout limit.
     *
     * @return The timeout for transmitting data via the opened connection (unit: milliseconds).
     */
    public int getSocketTimeoutInMillisecond() {
        return socketTimeoutInMillisecond;
    }

    /**
     * Set the timeout for transmitting data through an opened connection (unit: milliseconds). 
     * 0 means infinite wait (but it is not recommended). It is subject to the system's timeout limit.
     *
     * @param socketTimeoutInMillisecond The timeout for transmitting data through an opened connection (unit: milliseconds).
     */
    public void setSocketTimeoutInMillisecond(int socketTimeoutInMillisecond) {
        Preconditions.checkArgument(socketTimeoutInMillisecond > 0, "The socket timeout must be greater than 0.");
        this.socketTimeoutInMillisecond = socketTimeoutInMillisecond;
    }

    /**
     * Returns the timeout for establishing a connection (unit: milliseconds). 0 means infinite waiting (but not recommended). Subject to the system's timeout limit.
     *
     * @return The timeout for establishing a connection (unit: milliseconds).
     */
    public int getConnectionTimeoutInMillisecond() {
        return connectionTimeoutInMillisecond;
    }

    /**
     * Set the timeout for establishing a connection (unit: milliseconds). 0 means wait indefinitely (but not recommended). Subject to the system's timeout restrictions.
     *
     * @param connectionTimeoutInMillisecond Timeout for establishing a connection (unit: milliseconds).
     */
    public void setConnectionTimeoutInMillisecond(int connectionTimeoutInMillisecond) {
        Preconditions.checkArgument(connectionTimeoutInMillisecond > 0, "The connection timeout must be greater than 0.");
        this.connectionTimeoutInMillisecond = connectionTimeoutInMillisecond;
    }

    /**
     * Returns the number of threads in the thread pool used for executing error retries. 
     * This thread pool is a ScheduledExecutorService.
     *
     * @return The number of threads in the thread pool used for executing error retries.
     */
    public int getRetryThreadCount() {
        return retryThreadCount;
    }

    /**
     * Set the number of threads in the thread pool used for executing error retries. 
     * This thread pool is a ScheduledExecutorService.
     *
     * @param retryThreadCount The number of threads in the thread pool used for executing error retries.
     */
    public void setRetryThreadCount(int retryThreadCount) {
        Preconditions.checkArgument(retryThreadCount > 0, "The retry thread count must be greater than 0.");
        this.retryThreadCount = retryThreadCount;
    }

    /**
     * Returns whether the response content needs to be verified by MD5.
     *
     * @return Whether the MD5 content needs to be verified by MD5.
     */
    public boolean isEnableResponseContentMD5Checking() {
        return enableResponseContentMD5Checking;
    }

    /**
     * Whether to enable MD5 verification for the content of the response. If enabled,
     * the Client will calculate the MD5 value of the response data and compare it with the value in the x-ots-contentmd5 header of the response.
     *
     * @param enableResponseContentMD5Checking Whether to verify the MD5 of the response data
     */
    public void setEnableResponseContentMD5Checking(
            boolean enableResponseContentMD5Checking) {
        this.enableResponseContentMD5Checking = enableResponseContentMD5Checking;
    }

    /**
     * Returns the request retry policy of TableStore
     *
     * @return request retry policy
     */
    public RetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    /**
     * Set the request retry strategy for TableStore
     *
     * @param retryStrategy The request retry strategy for TableStore
     */
    public void setRetryStrategy(RetryStrategy retryStrategy) {
        Preconditions.checkArgument(retryStrategy != null, "The retry strategy should not be null.");
        this.retryStrategy = retryStrategy;
    }

    /**
     * Returns the currently set time threshold (unit: milliseconds).
     * When the total execution time of a request (including the time used for retries) exceeds this threshold, 
     * the SDK will log a WARN-level message.
     * This feature depends on the logging-related configurations.
     *
     * @return Time threshold
     */
    public int getTimeThresholdOfTraceLogger() {
        return timeThresholdOfTraceLogger;
    }

    /**
     * Set a time threshold (unit: milliseconds).
     * When the total execution time of a request (including the time used for retries) exceeds this threshold, 
     * the SDK will log a WARN level message.
     * This feature depends on the logging-related configurations.
     *
     * @param timeThresholdOfTraceLogger trace logger
     */
    public void setTimeThresholdOfTraceLogger(int timeThresholdOfTraceLogger) {
        Preconditions.checkArgument(timeThresholdOfTraceLogger > 0, "The time threshold of trace logger must be greater than 0.");
        this.timeThresholdOfTraceLogger = timeThresholdOfTraceLogger;
    }

    /**
     * Returns the current server-side Tracer time threshold setting (unit: milliseconds).
     * When a request takes longer than this threshold to execute on the server side, 
     * the SDK will receive tracer information from the server and record it.
     * This feature depends on relevant server-side configurations.
     *
     * @return Time threshold
     */
    public int getTimeThresholdOfServerTracer() {
        return timeThresholdOfServerTracer;
    }

    /**
     * Set the time threshold for the server Tracer (unit: milliseconds).
     * When the execution time of a request on the server exceeds this threshold, 
     * the SDK will receive tracer information from the server and record it.
     * This feature depends on the relevant server configuration.
     *
     * @param timeThresholdOfServerTracer trace logger
     */
    public void setTimeThresholdOfServerTracer(int timeThresholdOfServerTracer) {
        Preconditions.checkArgument(timeThresholdOfServerTracer > 0, "The time threshold of server tracer must be greater than 0.");
        this.timeThresholdOfServerTracer = timeThresholdOfServerTracer;
    }

    /**
     * Returns the proxy server host address.
     *
     * @return The proxy server host address.
     */
    public String getProxyHost() {
        return proxyHost;
    }

    /**
     * Set the proxy server host address.
     *
     * @param proxyHost The proxy server host address.
     */
    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    /**
     * Returns the proxy server port.
     *
     * @return The proxy server port.
     */
    public int getProxyPort() {
        return proxyPort;
    }

    /**
     * Set the proxy server port.
     *
     * @param proxyPort The proxy server port.
     */
    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    /**
     * Returns the username verified by the proxy server.
     *
     * @return Username.
     */
    public String getProxyUsername() {
        return proxyUsername;
    }

    /**
     * Set the username for proxy server authentication.
     *
     * @param proxyUsername The username.
     */
    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    /**
     * Returns the password for proxy server authentication.
     *
     * @return The password.
     */
    public String getProxyPassword() {
        return proxyPassword;
    }

    /**
     * Set the password for proxy server authentication.
     *
     * @param proxyPassword The password.
     */
    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    /**
     * Returns the Windows domain name for accessing the NTLM authenticated proxy server (optional).
     *
     * @return The domain name.
     */
    public String getProxyDomain() {
        return proxyDomain;
    }

    /**
     * Set the Windows domain name for accessing the NTLM authentication proxy server (optional).
     *
     * @param proxyDomain The domain name.
     */
    public void setProxyDomain(String proxyDomain) {
        this.proxyDomain = proxyDomain;
    }

    /**
     * Returns the Windows workstation name of the NTLM proxy server.
     *
     * @return The Windows workstation name of the NTLM proxy server.
     */
    public String getProxyWorkstation() {
        return proxyWorkstation;
    }

    /**
     * Set the Windows workstation name for the NTLM proxy server. (Optional, this parameter does not need to be set if the proxy server is not NTLM).
     *
     * @param proxyWorkstation The Windows workstation name for the NTLM proxy server.
     */
    public void setProxyWorkstation(String proxyWorkstation) {
        this.proxyWorkstation = proxyWorkstation;
    }

    /**
     * Get the maximum timeout for waiting asynchronous call returns in the sync Client.
     *
     * @return
     */
    public long getSyncClientWaitFutureTimeoutInMillis() {
        return syncClientWaitFutureTimeoutInMillis;
    }

    /**
     * Set the maximum timeout for waiting for asynchronous calls to return within the sync Client.
     *
     * @param syncClientWaitFutureTimeoutInMillis
     */
    public void setSyncClientWaitFutureTimeoutInMillis(long syncClientWaitFutureTimeoutInMillis) {
        this.syncClientWaitFutureTimeoutInMillis = syncClientWaitFutureTimeoutInMillis;
    }

    /**
     * Get the configuration of the time series Client.
     *
     * @return
     */
    public TimeseriesConfiguration getTimeseriesConfiguration() {
        return timeseriesConfiguration;
    }

    /**
     * Set the configuration for the timeseries Client.
     *
     * @param timeseriesConfiguration
     */
    public void setTimeseriesConfiguration(TimeseriesConfiguration timeseriesConfiguration) {
        this.timeseriesConfiguration = timeseriesConfiguration;
    }

    public int getConnectionRequestTimeoutInMillisecond() {
        return connectionRequestTimeoutInMillisecond;
    }

    /**
     * Set the ConnectionRequestTimeout configuration for HttpAsyncClient.
     *
     * @param connectionRequestTimeoutInMillisecond
     */
    public void setConnectionRequestTimeoutInMillisecond(int connectionRequestTimeoutInMillisecond) {
        this.connectionRequestTimeoutInMillisecond = connectionRequestTimeoutInMillisecond;
    }

    /**
     * Get the status of the link tracking system.
     */
    public boolean isEnableRequestTracer() {
        return enableRequestTracer;
    }

    /**
     * Set whether the link tracing system is enabled
     *
     * @param enableRequestTracer
     */
    public void setEnableRequestTracer(boolean enableRequestTracer) {
        this.enableRequestTracer = enableRequestTracer;
    }


    /**
     * Get the status of DNS cache
     */
    public boolean isEnableDnsCache() {
        return enableDnsCache;
    }

    /**
     * Set whether to enable DNS cache
     * @param enableDnsCache Control the configuration of whether to turn on DNS cache
     */
    public void setEnableDnsCache(boolean enableDnsCache) {
        this.enableDnsCache = enableDnsCache;
    }

    /**
     * Get the number of entries in the DNS cache
     */
    public int getDnsCacheMaxSize() {
        return dnsCacheMaxSize;
    }

    /**
     * Get the expiration time of DNS cache, in seconds.
     */
    public int getDnsCacheExpireAfterWriteSec() {
        return dnsCacheExpireAfterWriteSec;
    }

    /**
     * Set the expiration time for DNS cache, in seconds.
     */
    public void setDnsCacheExpireAfterWriteSec(int dnsCacheExpireAfterWriteSec) {
        Preconditions.checkArgument(dnsCacheExpireAfterWriteSec > 0, "The dns cache expire after write seconds must be greater than 0.");
        Preconditions.checkArgument(dnsCacheExpireAfterWriteSec <= 600, "The dns cache expire after write seconds must be less than or equal to 600s.");
        this.dnsCacheExpireAfterWriteSec = dnsCacheExpireAfterWriteSec;
    }

    /**
     * Get the refresh frequency of DNS cache, in seconds.
     */
    public int getDnsCacheRefreshAfterWriteSec() {
        return dnsCacheRefreshAfterWriteSec;
    }

    /**
     * Set the refresh frequency of DNS cache, in seconds.
     */
    public void setDnsCacheRefreshAfterWriteSec(int dnsCacheRefreshAfterWriteSec) {
        Preconditions.checkArgument(dnsCacheRefreshAfterWriteSec > 0, "The dns cache refresh after write seconds must be greater than 0.");
        Preconditions.checkArgument(dnsCacheRefreshAfterWriteSec < this.dnsCacheExpireAfterWriteSec, "The dns cache refresh after write seconds must be less than dnsCacheExpireAfterWriteSec.");
        this.dnsCacheRefreshAfterWriteSec = dnsCacheRefreshAfterWriteSec;
    }

    /**
     * Set the interface for the link tracing system
     *
     * @param requestTracer Interface for the link tracing system
     */
    public void setRequestTracer(RequestTracer requestTracer) {
        this.requestTracer = requestTracer;
    }

    /**
     * Get the link tracking system interface
     *
     * @return Implementation of the link tracking system interface
     */
    public RequestTracer getRequestTracer() {
        return requestTracer;
    }

    /**
     * Set the cache size of the ssl session
     *
     * @param sslSessionCacheSize the cache size of the ssl session
     */
    public void setSslSessionCacheSize(int sslSessionCacheSize) {
        Preconditions.checkArgument(sslSessionCacheSize >= 0, "SSL session cache size should be no less than 0.");
        this.sslSessionCacheSize = sslSessionCacheSize;
    }

    /**
     * Get the cache size of the ssl session
     *
     * @return sslSessionCacheSize
     */
    public int getSslSessionCacheSize() { return sslSessionCacheSize; }

    /**
     * Set the timeout for the SSL session
     *
     * @param seconds The timeout for the SSL session, 0 means no limit; the timeout should be no less than 0, in seconds
     */
    public void setSslSessionTimeoutInSec(int seconds) {
        Preconditions.checkArgument(seconds >= 0, "SSL session timeout should be no less than 0.");
        this.sslSessionTimeoutInSec = seconds;
    }

    /**
     * Get the timeout of the ssl session
     *
     * @return sslSessionTimeout The timeout of the ssl session, 0 means no limit, -1 means not configured; timeout is in seconds
     */
    public int getSslSessionTimeoutInSec() { return sslSessionTimeoutInSec; }
}
