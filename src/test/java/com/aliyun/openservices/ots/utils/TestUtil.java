package com.aliyun.openservices.ots.utils;

import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.OTSClient;
import com.aliyun.openservices.ots.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestUtil {

    public static long randomLong() {
        Random random = new Random(System.currentTimeMillis());
        return random.nextInt();
    }

    public static boolean randomBoolean() {
        Random random = new Random(System.currentTimeMillis());
        return random.nextInt() % 2 == 0;
    }

    public static double randomDouble() {
        Random random = new Random(System.currentTimeMillis());
        return random.nextDouble();
    }

    public static byte[] randomBytes(int length) {
        Random random = new Random(System.currentTimeMillis());
        byte[] result = new byte[length];
        random.nextBytes(result);
        return result;
    }

    public static String newTableName(String tableName) {
        return tableName + "_" + System.getProperty("java.version").replace(".", "_");
    }

    public static String randomString(int length) {
        Random random = new Random(System.currentTimeMillis());
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char)random.nextInt();
        }
        return new String(new String(chars).getBytes());
    }

    public static int randomLength() {
        int length = (int)(randomLong() % (1024));
        return Math.abs(length);
    }

    public static PrimaryKeyValue randomPrimaryKeyValue(PrimaryKeyType type) {
        switch (type) {
            case STRING:
                return PrimaryKeyValue.fromString(randomString(randomLength()));
            case INTEGER:
                return PrimaryKeyValue.fromLong(randomLong());
            default:
                throw new IllegalStateException("Unsupported primary key type: " + type);
        }
    }

    public static ColumnValue randomColumnValue(ColumnType type) {
        switch (type) {
            case STRING:
                return ColumnValue.fromString(randomString(randomLength()));
            case INTEGER:
                return ColumnValue.fromLong(randomLong());
            case BOOLEAN:
                return ColumnValue.fromBoolean(randomBoolean());
            case DOUBLE:
                return ColumnValue.fromDouble(randomDouble());
            case BINARY:
                return ColumnValue.fromBinary(randomBytes(randomLength()));
            default:
                throw new IllegalStateException("Unsupported column type: " + type);
        }
    }

    public static void DeleteTable(OTS ots, String tableName) {
        try {
            DeleteTableRequest deleteTableRequest = new DeleteTableRequest(tableName);
            ots.deleteTable(deleteTableRequest);
        } catch (Exception ex) {
            ;
        }
    }
}
