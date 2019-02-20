package com.alicloud.openservices.tablestore.common;

import com.alicloud.openservices.tablestore.model.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

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
            case BINARY:
                return PrimaryKeyValue.fromBinary(randomBytes(randomLength()));
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

    public static PrimaryKey randomPrimaryKey(PrimaryKeySchema[] schema) {
        PrimaryKeyColumn[] pks = new PrimaryKeyColumn[schema.length];
        for (int i = 0; i < schema.length; i++) {
            pks[i] = new PrimaryKeyColumn(schema[i].getName(), randomPrimaryKeyValue(schema[i].getType()));
        }
        return new PrimaryKey(pks);
    }

    public static PrimaryKeySchema[] randomPrimaryKeySchema(int size) {
        PrimaryKeySchema[] pks = new PrimaryKeySchema[size];
        for (int i = 0; i < size; i++) {
            String name = "pk" + i;
            switch (Math.abs((int)randomLong()) % 3) {
                case 0:
                    pks[i] = new PrimaryKeySchema(name, PrimaryKeyType.INTEGER);
                    break;
                case 1:
                    pks[i] = new PrimaryKeySchema(name, PrimaryKeyType.BINARY);
                    break;
                case 2:
                    pks[i] = new PrimaryKeySchema(name, PrimaryKeyType.STRING);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
        return pks;
    }

    public static Row randomRow(int pkCount, int columnCount) {
        PrimaryKey primaryKey = randomPrimaryKey(randomPrimaryKeySchema(pkCount));
        List<Column> columns = new ArrayList<Column>();
        for (int i = 0; i < columnCount; i++) {
            columns.add(new Column("column" + i, randomColumnValue(ColumnType.INTEGER), Math.abs(randomLong())));
        }
        return new Row(primaryKey, columns);
    }

    public static void compareRow(Row row1, Row row2) {
        assertEquals(row1.getPrimaryKey(), row2.getPrimaryKey());
        assertEquals(row1.getColumns().length, row2.getColumns().length);
        for (int i = 0; i < row1.getColumns().length; i++) {
            assertEquals(row1.getColumns()[i], row2.getColumns()[i]);
        }
    }

    private static Field getAccessibleField(Class<?> clazz, String fieldName)
            throws NoSuchFieldException {

        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field;
    }

    private static void injectIntoUnmodifiableMap(String key, String value, Object map) throws Exception {
        Class unmodifiableMap = Class.forName("java.util.Collections$UnmodifiableMap");
        Field field = getAccessibleField(unmodifiableMap, "m");
        Object obj = field.get(map);
        ((Map<String, String>) obj).put(key, value);
    }

    public static void injectEnvironmentVariable(String key, String value)
            throws Exception {

        Class<?> processEnvironment = Class.forName("java.lang.ProcessEnvironment");

        Field unmodifiableMapField = getAccessibleField(processEnvironment, "theUnmodifiableEnvironment");
        Object unmodifiableMap = unmodifiableMapField.get(null);
        injectIntoUnmodifiableMap(key, value, unmodifiableMap);

        Field mapField = getAccessibleField(processEnvironment, "theEnvironment");
        Map<String, String> map = (Map<String, String>) mapField.get(null);
        map.put(key, value);
    }
}
