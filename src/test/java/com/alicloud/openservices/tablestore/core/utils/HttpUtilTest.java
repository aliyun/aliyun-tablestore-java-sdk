package com.alicloud.openservices.tablestore.core.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HttpUtilTest {
    @Test
    public void testValidateEndpoint() {
        assertEquals(true, HttpUtil.validateEndpointArgs("http://test.cn-hangzhou.aliyuncs.com"));
        assertEquals(true, HttpUtil.validateEndpointArgs("http://test.cn-hangzhou.aliyuncs.com/"));
        assertEquals(true, HttpUtil.validateEndpointArgs("https://test.cn-hangzhou.aliyuncs.com"));
        assertEquals(true, HttpUtil.validateEndpointArgs("https://10.10.11.1"));
        assertEquals(true, HttpUtil.validateEndpointArgs("https://test"));
        assertEquals(true, HttpUtil.validateEndpointArgs("HTTP://test"));
        assertEquals(true, HttpUtil.validateEndpointArgs("HTTPs://test"));
        assertEquals(true, HttpUtil.validateEndpointArgs("HTTPS://test"));
        assertEquals(true, HttpUtil.validateEndpointArgs("https://test.cn-hangzhou.aliyuncs.com/"));
        assertEquals(true, HttpUtil.validateEndpointArgs("https://test.cn-hangzhou.aliyuncs.com:80/"));
        assertEquals(true, HttpUtil.validateEndpointArgs("https://test.cn-hangzhou.aliyuncs.com:80"));
        assertEquals(true, HttpUtil.validateEndpointArgs("https://test.cn-hangzhou.aliyuncs.com:8090/"));
        assertEquals(false, HttpUtil.validateEndpointArgs("https://test.cn-hangzhou.aliyuncs.com:8090a/"));
        assertEquals(false, HttpUtil.validateEndpointArgs("test.cn-hangzhou.aliyuncs.com/"));
        assertEquals(false, HttpUtil.validateEndpointArgs("http://test.cn-hangzhou.aliyuncs.com/s"));
        assertEquals(false, HttpUtil.validateEndpointArgs("https://test?.cn-hangzhou.aliyuncs.com/"));
    }

    @Test
    public void testSSRFCheck() {
        assertEquals(true, HttpUtil.checkSSRF("ots-zzf"));
        assertEquals(true, HttpUtil.checkSSRF("abcd"));
        assertEquals(true, HttpUtil.checkSSRF("12345"));
        assertEquals(true, HttpUtil.checkSSRF("abc_def"));
        assertEquals(true, HttpUtil.checkSSRF("Abcd0192_232-efe"));
        assertEquals(false, HttpUtil.checkSSRF("sda*2"));
    }
}
