package com.alicloud.openservices.tablestore.common;

public class OTSRestrictedItemConst {
    
    public static final int TABLE_NAME_LENGTH_MAX = 255;
    public static final int COLUMN_NAME_LENGTH_MAX  = 255;
    public static final int TABLE_NUMBER_MAX = 64;
    public static final int PRIMARY_KEY_COLUMN_NUMBER_MAX = 4;
    public static final int PRIMARY_KEY_VALUE_STRING_LENGTH_MAX = 1024;
    public static final int PRIMARY_KEY_VALUE_BINARY_LENGTH_MAX = 1024;
    public static final int COLUMN_VALUE_STRING_LENGTH_MAX = 2 * 1024 * 1024;
    public static final int COLUMN_VALUE_BINARY_LENGTH_MAX = 2 * 1024 * 1024;
    public static final int BATCH_GET_ROW_COUNT_MAX = 100;
    public static final int BATCH_WRITE_ROW_COUNT_MAX = 200;
    public static final int GET_RANGE_COUNT_MAX = 5000;
    public static final int GET_RANGE_SIZE_MAX = 4 * 1024 * 1024;
    public static final int COLUMN_COUNT_MAX_IN_SINGLE_ROW = 1024;
}
