/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import com.aliyun.openservices.ots.auth.ServiceCredentials;
import com.aliyun.openservices.ots.auth.ServiceSignature;
import com.aliyun.openservices.ots.comm.ResponseHandler;
import com.aliyun.openservices.ots.comm.ResponseMessage;
import com.aliyun.openservices.ots.utils.DateUtil;
import com.aliyun.openservices.ots.ClientException;
import static com.aliyun.openservices.ots.internal.OTSHTTPConstant.*;
import static com.aliyun.openservices.ots.internal.OTSUtil.OTS_RESOURCE_MANAGER;
import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;

/**
 * 验证返回结果。
 *
 */
public class OTSValidationResponseHandler implements ResponseHandler{

    private int responseExpireMinutes = OTSConsts.RESPONSE_EXPIRE_MINUTES;
    private ServiceCredentials credentials;
    private String otsAction;

    public OTSValidationResponseHandler(ServiceCredentials credentials, String otsAction){
        assertParameterNotNull(credentials, "credentials");
        assertParameterNotNull(otsAction, "otsAction");
        this.credentials = credentials;
        this.otsAction = otsAction;
    }

    public int getResponseExpireMinutes() {
        return responseExpireMinutes;
    }

    public void setResponseExpireMinutes(int responseExpireMinutes) {
        this.responseExpireMinutes = responseExpireMinutes;
    }

    public void handle(ResponseMessage responseData) throws ClientException {
        Map<String, String> headers = responseData.getHeadersMap();

        // 验证头信息完整性
        if (!headers.containsKey(OTS_HEADER_DATE)){
            throw OTSExceptionFactory.createResponseException(
                    OTS_RESOURCE_MANAGER.getFormattedString("MissingHeader", OTS_HEADER_DATE), null);
        }
        if (!headers.containsKey(OTS_HEADER_OTS_CONTENT_MD5)){
            throw OTSExceptionFactory.createResponseException(
                    OTS_RESOURCE_MANAGER.getFormattedString("MissingHeader", OTS_HEADER_OTS_CONTENT_MD5), null);
        }
        if (!headers.containsKey(OTS_HEADER_OTS_CONTENT_TYPE)){
            throw OTSExceptionFactory.createResponseException(
                    OTS_RESOURCE_MANAGER.getFormattedString("MissingHeader", OTS_HEADER_OTS_CONTENT_TYPE), null);
        }
        if (!headers.containsKey(OTS_HEADER_AUTHORIZATION)){
            throw OTSExceptionFactory.createResponseException(
                    OTS_RESOURCE_MANAGER.getFormattedString("MissingHeader", OTS_HEADER_AUTHORIZATION), null);
        }
        final String NEW_LINE = "\n";
        
        // 验证结果是否过期

        Date responseDate;

        try {
            responseDate = DateUtil.parseRfc822Date(headers.get(OTS_HEADER_DATE));
        } catch (IllegalArgumentException e) {
            throw OTSExceptionFactory.createResponseException("Parse date header from response failed.", e);
        } catch (UnsupportedOperationException e) {
            throw OTSExceptionFactory.createResponseException("Parse date header from response failed.", e);
        }
        Date now = new Date();
        long span = (now.getTime() - responseDate.getTime()) / (1000 * 60); // as minutes
        if (span > this.responseExpireMinutes){
            throw OTSExceptionFactory.createResponseException("The response has expired.", null);
        }

        // 验证授权信息
        StringBuilder canonicalizedOtsHeader = new StringBuilder(1000);
        Map<String, String> sortedMap = new TreeMap<String, String>();
        sortedMap.putAll(headers);
        for(String key : sortedMap.keySet()){
            if (key.startsWith(OTS_HEADER_PREFIX)){
                canonicalizedOtsHeader.append(key);
                canonicalizedOtsHeader.append(":");
                canonicalizedOtsHeader.append(sortedMap.get(key));
                canonicalizedOtsHeader.append(NEW_LINE);
            }
        }

        String canonicalizedResource = "/" + this.otsAction;

        String data = 
                canonicalizedOtsHeader.toString() +
                canonicalizedResource;
        String actualSign =
                ServiceSignature.create().computeSignature(
                        this.credentials.getAccessKeySecret(), data);

        String authHeader = headers.get(OTS_HEADER_AUTHORIZATION);
        boolean authEqual = false;
        if (authHeader.contains(":")){
            String[] arr = authHeader.split(":");
            authEqual = arr[arr.length - 1].endsWith(actualSign);
        }
        if (!authEqual){
            throw OTSExceptionFactory.createResponseException("返回结果授权信息验证失败。", null);
        }
    }
}
