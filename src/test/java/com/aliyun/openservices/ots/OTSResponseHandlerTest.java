/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.Map;

import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.junit.Test;

import com.aliyun.openservices.ots.auth.ServiceCredentials;
import com.aliyun.openservices.ots.comm.ResponseMessage;
import com.aliyun.openservices.ots.utils.DateUtil;
import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.internal.OTSValidationResponseHandler;

@SuppressWarnings("deprecation")
public class OTSResponseHandlerTest {
    @Test
    public void testValidationResponseHandler() throws UnsupportedEncodingException{
        ServiceCredentials cred =
                new ServiceCredentials("****", "******");
        OTSValidationResponseHandler h =
                new OTSValidationResponseHandler(cred, "CreateTable");

        ResponseMessage responseData = createTestResponseData();
        responseData.getHeadersMap().remove("x-ots-date");

        try{
            h.handle(responseData);
            fail("Expected exception not thrown.");
        } catch(Exception e){
            assertTrue(e instanceof ClientException);
        }
        
        responseData = createTestResponseData();
        responseData.getHeadersMap().put("x-ots-date", DateUtil.formatRfc822Date(new Date()));

        try{
            h.handle(responseData);
            fail("Expected exception not thrown.");
        } catch(Exception e){
            assertTrue(e instanceof ClientException);
        }

        responseData = createTestResponseData();

        // response expired.
        try{
            h.handle(responseData);
            fail("Expected exception not thrown.");
        } catch(Exception e){
            assertTrue(e instanceof ClientException);
        }

        responseData = createTestResponseData();
        h.setResponseExpireMinutes(Integer.MAX_VALUE);
        try {
            h.handle(responseData);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Should not throw exception.");
        }
    }

    private ResponseMessage createTestResponseData() throws UnsupportedEncodingException {
        ResponseMessage responseData = new ResponseMessage(new BasicHttpResponse(null, 200, null));
        String uri = "http://service.ots.aliyun.com/CreateTable";

        Map<String, String> headers = responseData.getHeadersMap();
        headers.put("Server", "AliyunOTS");
        headers.put("Authorization", "OTS rr0yssqhctx2lz9r4o29uu8i:JJQMflH/Vs38p/7k8bzuJkEaT3s=");
        headers.put("x-ots-contentmd5", "1B2M2Y8AsgTpgAmY7PhCfg==");
        headers.put("x-ots-contenttype", "text");
        headers.put("x-ots-hostid", "MTAuMjMwLjIwMS4yNQ==");
        headers.put("x-ots-requestid", "1624561984-1373978089964071");
        headers.put("x-ots-date", "Fri, 03 Feb 2012 06:42:22 GMT");
        headers.put("Content-Length", "216");
        
        String content = "";
        responseData.getResponse().setEntity(new StringEntity(content));
        return responseData;
    }

}
