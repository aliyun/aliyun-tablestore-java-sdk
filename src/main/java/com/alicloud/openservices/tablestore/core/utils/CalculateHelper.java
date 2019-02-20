package com.alicloud.openservices.tablestore.core.utils;

import java.io.UnsupportedEncodingException;

public class CalculateHelper {
    
    /**
     * 计算字符串的大小(按照UTF-8编码)
     * @param str
     * @return 返回字符串的字节数
     * @throws IllegalStateException
     */
    public static int calcStringSizeInBytes(String str) throws IllegalStateException {
        try {
            return str.getBytes("UTF-8").length;
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e.getMessage(), e.getCause());
        }
    }
}
