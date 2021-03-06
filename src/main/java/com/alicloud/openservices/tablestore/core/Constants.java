package com.alicloud.openservices.tablestore.core;

import java.nio.charset.Charset;

public class Constants {
    // request headers
    public static final String USER_AGENT = "ots-java-sdk 4.1.0";
    public static final String OTS_HEADER_API_VERSION = "x-ots-apiversion";
    public static final String OTS_HEADER_ACCESS_KEY_ID = "x-ots-accesskeyid";
    public static final String OTS_HEADER_OTS_CONTENT_MD5 = "x-ots-contentmd5";
    public static final String OTS_HEADER_SIGNATURE = "x-ots-signature";
    public static final String OTS_HEADER_INSTANCE_NAME = "x-ots-instancename";
    public static final String OTS_HEADER_SDK_TRACE_ID = "x-ots-sdk-traceid";
    public static final String OTS_HEADER_TRACE_THRESHOLD = "x-ots-trace-threshold";
    public static final String OTS_HEADER_DATE = "x-ots-date";
    public static final String OTS_HEADER_STS_TOKEN = "x-ots-ststoken";

    // response headers
    public static final String OTS_HEADER_PREFIX = "x-ots-";
    public static final String OTS_HEADER_OTS_CONTENT_TYPE = "x-ots-contenttype";
    public static final String OTS_HEADER_AUTHORIZATION = "Authorization";
    public static final String OTS_HEADER_REQUEST_ID = "x-ots-requestid";
    public static final String OTS_MOVED_PERMANENTLY_LOCATION = "Location";
    public static final String OTS_HEADER_TRACE_INFO = "x-ots-traceinfo";
    
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

    /**
     * OTS RESTful API的版本号。
     */
    public static final String API_VERSION = "2015-12-31";

    /**
     * OTS请求的编码方法。
     */
    public static final String UTF8_ENCODING = "utf-8";
    public static final String HTTP_HEADER_ENCODING = "iso-8859-1";
    public static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
}
