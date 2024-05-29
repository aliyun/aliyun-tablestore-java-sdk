package com.alicloud.openservices.tablestore.core;

import java.nio.charset.Charset;

public class Constants {
    // ALL HTTP HEADERS SHOULD BE DEFINED IN LOWERCASE
    // request headers
    public static final String USER_AGENT = "ots-java-sdk 5.17.1";
    public static final String OTS_HEADER_API_VERSION = "x-ots-apiversion";
    public static final String OTS_HEADER_ACCESS_KEY_ID = "x-ots-accesskeyid";
    public static final String OTS_HEADER_OTS_CONTENT_MD5 = "x-ots-contentmd5";
    public static final String OTS_HEADER_SIGNATURE = "x-ots-signature";
    public static final String OTS_HEADER_SIGNATURE_V4 = "x-ots-signaturev4";
    public static final String OTS_HEADER_SIGN_DATE = "x-ots-signdate";
    public static final String OTS_HEADER_SIGN_REGION = "x-ots-signregion";
    public static final String OTS_HEADER_INSTANCE_NAME = "x-ots-instancename";
    public static final String OTS_HEADER_SDK_TRACE_ID = "x-ots-sdk-traceid";
    public static final String OTS_HEADER_TRACE_THRESHOLD = "x-ots-trace-threshold";
    public static final String OTS_HEADER_DATE = "x-ots-date";
    public static final String OTS_HEADER_STS_TOKEN = "x-ots-ststoken";
    public static final String OTS_HEADER_REQUEST_PRIORITY = "x-ots-request-priority";
    public static final String OTS_HEADER_REQUEST_TAG = "x-ots-request-tag";

    // response headers
    public static final String OTS_HEADER_PREFIX = "x-ots-";
    public static final String OTS_HEADER_OTS_CONTENT_TYPE = "x-ots-contenttype";
    public static final String OTS_HEADER_AUTHORIZATION = "authorization";
    public static final String OTS_HEADER_REQUEST_ID = "x-ots-requestid";
    public static final String OTS_MOVED_PERMANENTLY_LOCATION = "location";
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

    public static final String ENDPOINT_REGEX = "^(?i)http(s)?(?-i)://[a-zA-Z0-9._-]+(:\\d{1,5})?(/)?$";
    public static final String SSRF_CHECK_REGEX = "^[a-zA-Z0-9_-]+$";
    public static final String ACCESSKEYID_REGEX = "^[a-zA-Z0-9+\\./_-]+$";

    /**
     * Timeseries隐藏主键数据格式
     */
    public static final String TIMESERIES_HIDDEN_PK = "_#h";

    /**
     * v4签名算签前对待算签字段加盐的盐值
     * signature = Base64(Hmac-SHA256(v4SigningKey, (stringToString + salt)))
     */
    public static final String V4_SIGNATURE_SALT = "ots";
    /**
     * v4签名算签的方法
     * signature = Base64(Hmac-SHA256(v4SigningKey, (stringToString + salt)))
     */
    public static final String SIGNING_KEY_SIGN_METHOD = "HmacSHA256";
    /**
     * v4签名的产品码
     */
    public static final String PRODUCT = "ots";
}
