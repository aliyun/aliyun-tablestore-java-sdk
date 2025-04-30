package com.alicloud.openservices.tablestore.core.http;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.aliyun.ots.thirdparty.org.apache.http.HttpResponse;
import com.aliyun.ots.thirdparty.org.apache.http.ProtocolVersion;
import com.aliyun.ots.thirdparty.org.apache.http.message.BasicHttpResponse;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.alicloud.openservices.tablestore.core.Constants.*;
import static org.junit.Assert.*;

public class OTSValidationResponseHandlerTest {

    private ServiceCredentials generateCredentials() {
        return new DefaultCredentials("ak","sk");
    }

    private OTSUri generateOTSUri() {
        return new OTSUri("http://endpoint.com","action");
    }

    private OTSValidationResponseHandler generateOTSValidationResponseHandler() {
        return new OTSValidationResponseHandler(generateCredentials(), generateOTSUri());
    }

    private ResponseMessage generateResponseMessage(Map<String,String> headers) {
        HttpResponse httpResponse = new BasicHttpResponse(new ProtocolVersion("http", 1,0), 200, "");
        for (Map.Entry<String,String> entry : headers.entrySet()) {
            httpResponse.addHeader(entry.getKey(), entry.getValue());
        }
        return new ResponseMessage(httpResponse);
    }

    @Test
    public void testHandleWithIncorrectSign(){
        Map<String, String> headers = new HashMap<>();
        headers.put(OTS_HEADER_OTS_CONTENT_MD5,"md5");
        headers.put(OTS_HEADER_OTS_CONTENT_TYPE,"json");
        headers.put(OTS_HEADER_AUTHORIZATION,"abcd");
        OTSValidationResponseHandler validationResponseHandler = generateOTSValidationResponseHandler();
        ClientException catchException = null;
        try {
            validationResponseHandler.handle(generateResponseMessage(headers));
        }catch (ClientException e) {
            catchException = e;
        }
        assertNotNull(catchException);
        assertTrue(catchException.getMessage().contains("Validate response authorization failed"));
    }

    @Test
    public void testHandleWithCorrectSign() {
        Map<String, String> headers = new HashMap<>();
        headers.put(OTS_HEADER_OTS_CONTENT_MD5,"md5");
        headers.put(OTS_HEADER_OTS_CONTENT_TYPE,"json");
        headers.put(OTS_HEADER_AUTHORIZATION,"auth:yxS9NtV7YZ+VtKXGkyeLOlYsH5o=");
        OTSValidationResponseHandler validationResponseHandler = generateOTSValidationResponseHandler();
        ClientException catchException = null;
        try {
            validationResponseHandler.handle(generateResponseMessage(headers));
        }catch (ClientException e) {
            catchException = e;
        }
        assertNull(catchException);
    }
    @Test
    public void testHandleWithNoSeparator(){
        Map<String, String> headers = new HashMap<>();
        headers.put(OTS_HEADER_OTS_CONTENT_MD5,"md5");
        headers.put(OTS_HEADER_OTS_CONTENT_TYPE,"json");
        headers.put(OTS_HEADER_AUTHORIZATION,"yxS9NtV7YZ+VtKXGkyeLOlYsH5o=");
        OTSValidationResponseHandler validationResponseHandler = generateOTSValidationResponseHandler();
        ClientException catchException = null;
        try {
            validationResponseHandler.handle(generateResponseMessage(headers));
        }catch (ClientException e) {
            catchException = e;
        }
        assertNotNull(catchException);
        assertTrue(catchException.getMessage().contains("Validate response authorization failed"));
    }

    @Test
    public void testHandleWithSignIsNotLastPart(){
        Map<String, String> headers = new HashMap<>();
        headers.put(OTS_HEADER_OTS_CONTENT_MD5,"md5");
        headers.put(OTS_HEADER_OTS_CONTENT_TYPE,"json");
        headers.put(OTS_HEADER_AUTHORIZATION,"auth:yxS9NtV7YZ+VtKXGkyeLOlYsH5o=***");
        OTSValidationResponseHandler validationResponseHandler = generateOTSValidationResponseHandler();
        ClientException catchException = null;
        try {
            validationResponseHandler.handle(generateResponseMessage(headers));
        }catch (ClientException e) {
            catchException = e;
        }
        assertNotNull(catchException);
        assertTrue(catchException.getMessage().contains("Validate response authorization failed"));
    }
}
