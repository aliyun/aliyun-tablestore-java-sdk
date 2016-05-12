/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.internal;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.aliyun.openservices.ots.auth.RequestSigner;
import com.aliyun.openservices.ots.auth.ServiceCredentials;
import com.aliyun.openservices.ots.auth.ServiceSignature;
import com.aliyun.openservices.ots.comm.RequestMessage;
import com.aliyun.openservices.ots.utils.DateUtil;
import com.aliyun.openservices.ots.utils.HttpUtil;
import com.aliyun.openservices.ots.ClientException;
import org.apache.http.Header;

import static com.aliyun.openservices.ots.internal.OTSHTTPConstant.*;
import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;


public class OTSRequestSigner implements RequestSigner {

    private String otsAction;
    private ServiceCredentials credentials;

    public OTSRequestSigner(String otsAction, ServiceCredentials credentials) {
        assertParameterNotNull(otsAction, "otsAction");
        assertParameterNotNull(credentials, "credentials");

        this.otsAction = otsAction;
        this.credentials = credentials;
    }

    @Override
    public void sign(RequestMessage request) throws ClientException {
        try {
            String signature = getSignature(otsAction, request.getRequest().getMethod(), request.getQueryParameters(), request.getRequest().getAllHeaders(), credentials);
            request.addHeader(OTS_HEADER_SIGNATURE, signature);
        } catch (UnsupportedEncodingException e) {
            throw new ClientException("无法计算签名：" + e.getMessage());
        }
    }

    private static String getSignature(String action, String method, Map<String, String> queryParameters, Header[] headers, ServiceCredentials credentials)
            throws UnsupportedEncodingException{
        StringBuilder canonicalizedOtsHeader = new StringBuilder(1000);
        Map<String, String> headerMap = new HashMap<String, String>();
        for (Header header : headers) {
            headerMap.put(header.getName(), header.getValue());
        }
        Map<String, String> sortedMap = sortMap(headerMap);
        for(String key : sortedMap.keySet()){
            if (key.startsWith(OTS_HEADER_PREFIX)){
                canonicalizedOtsHeader.append(key).append(":").append(sortedMap.get(key).trim()).append("\n");
            }
        }

        if (queryParameters == null) {
            queryParameters = new HashMap<String, String>();
        }

        StringBuilder sb = new StringBuilder(1000);
        sb.append("/").append(action).append("\n").append(method).append("\n").
            append(queryParameters.size() == 0 ? "" : HttpUtil.paramToQueryString(sortMap(queryParameters), OTSConsts.DEFAULT_ENCODING)).
            append("\n").
            append(canonicalizedOtsHeader.toString());
        
        return ServiceSignature.create().computeSignature(credentials.getAccessKeySecret(), sb.toString());
    }

    private static Map<String, String> sortMap(Map<String, String> unsortMap) {
        //put sorted list into map again
        Map<String, String> sortedMap = new TreeMap<String, String>();
        sortedMap.putAll(unsortMap);
        return sortedMap;
    }
}
