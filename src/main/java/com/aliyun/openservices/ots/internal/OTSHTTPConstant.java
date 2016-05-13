package com.aliyun.openservices.ots.internal;

public class OTSHTTPConstant {
    // request headers
    public static final String OTS_HEADER_API_VERSION = "x-ots-apiversion";
    public static final String OTS_HEADER_ACCESS_KEY_ID = "x-ots-accesskeyid";
    public static final String OTS_HEADER_STS_TOKEN = "x-ots-ststoken";
    public static final String OTS_HEADER_OTS_CONTENT_MD5 = "x-ots-contentmd5";
    public static final String OTS_HEADER_SIGNATURE = "x-ots-signature";
    public static final String OTS_HEADER_INSTANCE_NAME = "x-ots-instancename";
    public static final String OTS_HEADER_SDK_TRACE_ID = "x-ots-sdk-traceid";

    // response headers
    public static final String OTS_HEADER_PREFIX = "x-ots-";
    public static final String OTS_HEADER_DATE = "x-ots-date";
    public static final String OTS_HEADER_OTS_CONTENT_TYPE = "x-ots-contenttype";
    public static final String OTS_HEADER_AUTHORIZATION = "Authorization";
    public static final String OTS_HEADER_REQUEST_ID = "x-ots-requestid";
    public static final String OTS_MOVED_PERMANENTLY_LOCATION = "Location";
    
    // request/response compress headers
    public final static String OTS_HEADER_REQUEST_COMPRESS_TYPE = "x-ots-request-compress-type";
    public final static String OTS_HEADER_REQUEST_COMPRESS_SIZE = "x-ots-request-compress-size";
    public final static String OTS_HEADER_RESPONSE_COMPRESS_TYPE = "x-ots-response-compress-type";
    public final static String OTS_HEADER_RESPONSE_COMPRESS_SIZE = "x-ots-response-compress-size";
    
    // compression type
    public final static String OTS_COMPRESS_TYPE = "deflate";
    
    // http status code
    public final static int OTS_HTTP_MOVED_PERMANENTLY = 301;
    public final static int OTS_HTTP_ENTITY_TOO_LARGE = 413;

}
