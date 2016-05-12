/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.aliyun.openservices.ots.comm.ResponseHandler;
import com.aliyun.openservices.ots.comm.ResponseMessage;
import com.aliyun.openservices.ots.utils.BinaryUtil;
import com.aliyun.openservices.ots.utils.IOUtils;
import com.aliyun.openservices.ots.utils.ResourceManager;
import com.aliyun.openservices.ots.utils.ServiceConstants;
import com.aliyun.openservices.ots.ClientException;
import org.apache.http.entity.ByteArrayEntity;

import static com.aliyun.openservices.ots.internal.OTSHTTPConstant.*;
import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;

/**
 * 验证返回结果。
 *
 */
public class OTSContentMD5ResponseHandler implements ResponseHandler{
    public OTSContentMD5ResponseHandler(){
    }

    public void handle(ResponseMessage responseData) throws ClientException {
        Map<String, String> headers = responseData.getHeadersMap();

        // 验证头信息完整性
        if (!headers.containsKey(OTS_HEADER_OTS_CONTENT_MD5)){
            throw OTSExceptionFactory.createResponseException(
                    OTS_RESOURCE_MANAGER.getFormattedString("MissingHeader", OTS_HEADER_OTS_CONTENT_MD5), null);
        }
        String contentMd5 = headers.get(OTS_HEADER_OTS_CONTENT_MD5);
        
        // 验证返回值MD5值是否正确
        byte[] content = null;
        String md5 = null;
        InputStream dataStream = null;
        try {
            dataStream = responseData.getContent();
        } catch (IOException e) {
            throw new ClientException("Can not read response from server.", e);
        }
        if (dataStream == null){
            throw OTSExceptionFactory.createResponseException(
                    ResourceManager.getInstance(ServiceConstants.RESOURCE_NAME_COMMON)
                        .getString("ServerReturnsUnknownError"),
                    null);
        }
        try {
            content = IOUtils.readStreamAsBytesArray(dataStream);
            md5 = BinaryUtil.toBase64String(BinaryUtil.calculateMd5(content));
        } catch (Exception e) {
            throw OTSExceptionFactory.createResponseException(
                    ResourceManager.getInstance(ServiceConstants.RESOURCE_NAME_COMMON)
                        .getString("ServerReturnsUnknownError"),
                    null);
        }
        if (!md5.equals(contentMd5)) {
            throw new ClientException(OTS_RESOURCE_MANAGER.getString("ResponseContentMD5Invalid"));
        }
        
        // 检查通过
        IOUtils.safeClose(dataStream);
        responseData.getResponse().setEntity(new ByteArrayEntity(content)); // reset output stream
    }
}