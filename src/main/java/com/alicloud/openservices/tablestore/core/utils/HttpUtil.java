/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.alicloud.openservices.tablestore.core.utils;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Map.Entry;

public class HttpUtil {

    /**
     * Encode a URL segment with special chars replaced.
     * @param value
     * @param charset
     * @return encoded url
     * @throws UnsupportedEncodingException
     */
    // TODO change the method name to percentageEncode
    public static String urlEncode(String value, String charset)
            throws UnsupportedEncodingException{
        return value != null ?
                URLEncoder.encode(value, charset).replace("+", "%20")
                    .replace("*", "%2A").replace("%7E", "~")
                : null;
    }

    /**
     * Encodes request parameters to a URL query.
     * @param params
     * @param charset
     * @return encoded query string
     * @throws UnsupportedEncodingException
     */
    public static String paramToQueryString(Map<String, String> params, String charset)
            throws UnsupportedEncodingException{
        if (params == null || params.size() == 0){
            return null;
        }

        StringBuilder paramString = new StringBuilder();
        boolean first = true;
        for(Entry<String, String> p : params.entrySet()){
            String key = p.getKey();
            String val = p.getValue();

            if (!first){
                paramString.append("&");
            }

            paramString.append(key);
            if (val != null){
                // The query string in URL should be encoded with URLEncoder standard.
                paramString.append("=").append(HttpUtil.urlEncode(val, charset));
                // TODO: Should use URLEncoder.encode(val, charset)) instead of HttpUril#urlEncode;
            }

            first = false;
        }

        return paramString.toString();
    }
    
}
