package com.alicloud.openservices.tablestore.core.http;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.aliyun.ots.thirdparty.org.apache.http.entity.ByteArrayEntity;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.Constants;
import com.alicloud.openservices.tablestore.core.utils.Base64;
import com.alicloud.openservices.tablestore.core.utils.BinaryUtil;
import com.alicloud.openservices.tablestore.core.utils.IOUtils;

public class ContentMD5ResponseHandler implements ResponseHandler {
    public void handle(ResponseMessage responseData) throws ClientException {
        Map<String, String> headers = responseData.getLowerCaseHeadersMap();

        // Verify the integrity of the header information
        String contentMd5 = headers.get(Constants.OTS_HEADER_OTS_CONTENT_MD5);
        if (contentMd5 == null) {
            throw new ClientException("Required header is not found: " + Constants.OTS_HEADER_OTS_CONTENT_MD5);
        }
        
        // Verify whether the returned MD5 value is correct
        byte[] content = null;
        String md5 = null;

        InputStream dataStream = null;

        try {
            dataStream = responseData.getContent();
        } catch (IOException e) {
            throw new ClientException("Can not read response from server.", e);
        }

        if (dataStream == null){
            throw new ClientException("The server returns an unknown error.");
        }

        try {
            content = IOUtils.readStreamAsBytesArray(dataStream);
            md5 = Base64.toBase64String(BinaryUtil.calculateMd5(content));
        } catch (Exception e) {
            throw new ClientException("The server returns an unknown error.");
        }
        if (!md5.equals(contentMd5)) {
            throw new ClientException("The MD5 value of response content is not equal with the value in header.");
        }
        
        // Check passed
        IOUtils.safeClose(dataStream);
        responseData.getResponse().setEntity(new ByteArrayEntity(content)); // reset output stream
    }
}
