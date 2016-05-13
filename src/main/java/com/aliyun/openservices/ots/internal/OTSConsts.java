package com.aliyun.openservices.ots.internal;

public interface OTSConsts {
    /**
     * 默认的OTS Endpoint。
     */
    public static final String DEFAULT_OTS_ENDPOINT = "http://ots.aliyuncs.com";
    
    /**
     * OTS RESTful API的版本号。
     */
    public static final String API_VERSION = "2014-08-08";
    
    /**
     * OTS请求的编码方法。
     */
    public static final String DEFAULT_ENCODING = "utf-8";

    /**
     * OTS请求返回结果的失效时间。
     */
    public static final int RESPONSE_EXPIRE_MINUTES = 15; // 表示返回结果的过期时间。

}
