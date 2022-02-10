package com.alicloud.openservices.tablestore.core.utils;

public class NumberUtils {

    public static int longToInt(long value) {
        if ((int) value != value) {
            throw new ArithmeticException("integer overflow");
        }
        return (int) value;
    }
}
