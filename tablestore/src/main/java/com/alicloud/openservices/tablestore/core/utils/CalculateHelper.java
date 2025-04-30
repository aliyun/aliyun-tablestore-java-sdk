package com.alicloud.openservices.tablestore.core.utils;

import java.io.UnsupportedEncodingException;

public class CalculateHelper {
    
    /**
     * Calculate the size of a string (based on UTF-8 encoding)
     * @param str
     * @return the number of bytes in the string
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
