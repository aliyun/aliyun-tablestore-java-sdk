/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots;

import com.aliyun.openservices.ots.internal.OTSDefaultRetryStrategy;
import com.aliyun.openservices.ots.internal.OTSRetryStrategy;

/**
 * 一些OTS服务特定相关的配置
 *
 */
public class OTSServiceConfiguration {

    private boolean enableRequestCompression = false;
    private boolean enableResponseCompression = false;
    private boolean enableResponseContentMD5Checking = false;
    private boolean enableResponseValidation = true;
    private OTSRetryStrategy retryStrategy = new OTSDefaultRetryStrategy();
    private int timeThresholdOfTraceLogger = 1000;
    
    public OTSServiceConfiguration() {
        
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
     * 是否需要告知OTS对返回的响应内容进行压缩 
     * @param enableResponseCompression
     *          是否告知OTS对返回的响应内容进行压缩
     */
    public void setEnableResponseCompression(boolean enableResponseCompression) {
        this.enableResponseCompression = enableResponseCompression;
    }
    
    /**
     * 返回是否需要告知OTS对返回的响应内容进行压缩
     * @return
     *      是否需要告知OTS对返回的响应内容进行压缩
     */ 
    public boolean isEnableResponseCompression() {
        return enableResponseCompression;
    }
    
    /**
     * 返回是否需要对响应内容做MD5校验
     * @return
     *      是否需要对MD5内容做MD5校验
     */
    public boolean isEnableResponseContentMD5Checking() {
        return enableResponseContentMD5Checking;
    }
    
    /**
     * 是否需要对响应的内容做MD5校验， 如果需要校验， 
     * Client会计算响应数据的MD5值并与返回的响应头中的Content-Md5头的值进行比对
     * @param enableResponseContentMD5Checking
     *          是否需要校验响应数据MD5
     */
    public void setEnableResponseContentMD5Checking(
            boolean enableResponseContentMD5Checking) {
        this.enableResponseContentMD5Checking = enableResponseContentMD5Checking;
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
     * 返回OTS的请求重试策略
     * @return 请求重试策略
     */
    public OTSRetryStrategy getRetryStrategy() {
        return retryStrategy;
    }

    /**
     * 设置OTS的请求重试策略
     * @param retryStrategy
     *          OTS的请求重试策略
     */
    public void setRetryStrategy(OTSRetryStrategy retryStrategy) {
        this.retryStrategy = retryStrategy;
    }

    /**
     * 返回当前设置的时间阈值(单位：毫秒)。
     * 当一个请求的总执行时间(包含重试占用的时间)超过该阈值时，SDK会记录一条WARN级别的日志。
     * 该功能依赖于日志相关的配置。
     * @return
     */
    public int getTimeThresholdOfTraceLogger() {
        return timeThresholdOfTraceLogger;
    }

    /**
     * 设置一个时间阈值(单位：毫秒)。
     * 当一个请求的总执行时间(包含重试占用的时间)超过该阈值时，SDK会记录一条WARN级别的日志。
     * 该功能依赖于日志相关的配置。
     * 
     * @param timeThresholdOfTraceLogger
     */
    public void setTimeThresholdOfTraceLogger(int timeThresholdOfTraceLogger) {
        this.timeThresholdOfTraceLogger = timeThresholdOfTraceLogger;
    }

}
