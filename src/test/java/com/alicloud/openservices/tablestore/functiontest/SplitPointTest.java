package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.InternalClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.model.*;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class  SplitPointTest {
    static String tableName = "TestPartition";
    static InternalClient client = null;

    private List<PrimaryKeyValue> getPoints() throws Exception {
        DescribeTableRequest request = new DescribeTableRequest(tableName);
        DescribeTableResponse response = client.describeTable(request, null).get();

        List<PrimaryKey> pointsKey = response.getShardSplits();
        List<PrimaryKeyValue> points = new ArrayList<PrimaryKeyValue>();
        //System.out.println(pointsKey);
        for (PrimaryKey pointKey : pointsKey){
            points.add(pointKey.getPrimaryKeyColumn(0).getValue());
        }
        return points;
    }

    private void deleteTable() throws Exception {
        DeleteTableRequest request = new DeleteTableRequest(tableName);
        try {
            client.deleteTable(request, null).get();
        } catch (TableStoreException e){
            assertEquals("Requested table does not exist.", e.getMessage());
        }
    }

    @BeforeClass
    public static void classBefore() {
        ServiceSettings settings = ServiceSettings.load();

        client = new InternalClient(settings.getOTSEndpoint(), settings.getOTSAccessKeyId(),
                settings.getOTSAccessKeySecret(), settings.getOTSInstanceName());
    }

    @AfterClass
    public static void classAfter() {
        client.shutdown();
    }


    @Test
    public void testPartitionIntegerPrimaryKey() throws Exception {

        deleteTable();

        TableMeta tableMetaInt = new TableMeta(tableName);
        tableMetaInt.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));
        TableMeta tableMetaStr = new TableMeta(tableName);
        tableMetaStr.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));

        int timeToLive = -1;
        int maxVersions = 3;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        //A、预分区分裂点只有单个数字的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromLong(10000)
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaInt, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePoints = getPoints();
            assertEquals(points, tablePoints);
            deleteTable();
        }

        //B、预分区分裂点含有多个数字的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromLong(10000),
                    PrimaryKeyValue.fromLong(20000),
                    PrimaryKeyValue.fromLong(30000)
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaInt, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePoints = getPoints();
            assertEquals(points, tablePoints);
            deleteTable();
        }

        //C、创建多个分区，超出server端分区限制，抛出异常
        {
            List<PrimaryKeyValue> points = new ArrayList<PrimaryKeyValue>();
            for (int i = 0; i < 64; i++) {
                points.add(PrimaryKeyValue.fromLong(i));
            }
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaInt, tableOptions);
            request.setSplitPoints(points);
            try {
                client.createTableEx(request, null).get();
                fail();
            } catch (TableStoreException e) {
                assertEquals("The count of partitions cannot be greater than 10", e.getMessage());
            }
        }

        //D、预分区分裂点含有Long.MIN_VALUE，抛出异常
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromLong(Long.MIN_VALUE),
                    PrimaryKeyValue.fromLong(20000),
                    PrimaryKeyValue.fromLong(30000)
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaInt, tableOptions);
            request.setSplitPoints(points);
            try {
                client.createTableEx(request, null).get();
                fail();
            } catch (TableStoreException e) {
                assertEquals("The value of partition range can not be INT64_MIN or INT64_MAX.", e.getMessage());
            }
        }

        //E、预分区分裂点含有Long.MAX_VALUE，抛出异常
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromLong(10000),
                    PrimaryKeyValue.fromLong(20000),
                    PrimaryKeyValue.fromLong(Long.MAX_VALUE)
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaInt, tableOptions);
            request.setSplitPoints(points);

            try {
                client.createTableEx(request, null).get();
                fail();
            } catch (TableStoreException e) {
                assertEquals("The value of partition range can not be INT64_MIN or INT64_MAX.", e.getMessage());
            }
        }

        //F、设置分裂点后，通过setTableMeta修改PrimaryKey的属性
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromLong(10000),
                    PrimaryKeyValue.fromLong(20000),
                    PrimaryKeyValue.fromLong(30000)
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaInt, tableOptions);
            request.setSplitPoints(points);
            request.setTableMeta(tableMetaStr);
            try {
                client.createTableEx(request, null).get();
                fail();
            } catch (TableStoreException e) {
                assertEquals("The value type of partition does not match with primary key.", e.getMessage());
            }
        }


    }

    @Test
    public void testPartitionStringPrimaryKey() throws Exception {

        deleteTable();

        TableMeta tableMetaStr = new TableMeta(tableName);
        tableMetaStr.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.STRING));

        int timeToLive = -1;
        int maxVersions = 3;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        //A、预分区分裂点只有单个英文字符串的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString("Chinese")
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePointsA = getPoints();
            assertEquals(points, tablePointsA);
            deleteTable();
        }
        //B、预分区分裂点含有多个英文字符串的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString("Chinese"),
                    PrimaryKeyValue.fromString("England"),
                    PrimaryKeyValue.fromString("France ")
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePoints = getPoints();
            assertEquals(points, tablePoints);
            deleteTable();
        }

        //C、预分区分裂点含有DataSize=64的英文字符串的表格创建
        {
            char[] bigChar;
            Arrays.fill(bigChar = new char[64], 'Z');
            String bigString = String.valueOf(bigChar);
            PrimaryKeyValue pointBigString = PrimaryKeyValue.fromString(bigString);

            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString("Chinese"),
                    PrimaryKeyValue.fromString("England"),
                    PrimaryKeyValue.fromString("France "),
                    pointBigString
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePoints = getPoints();
            assertEquals(points, tablePoints);
            deleteTable();
        }

        //D、 预分区分裂点只有单个中文字符串的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString("中国")
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePoints = getPoints();
            assertEquals(points, tablePoints);
            deleteTable();
        }

        //E、预分区分裂点含有多个中文字符串的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString("中国"),
                    PrimaryKeyValue.fromString("法国"),
                    PrimaryKeyValue.fromString("英国")
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePoints = getPoints();
            assertEquals(points, tablePoints);
            deleteTable();
        }
        //F、预分区分裂点含有DataSize=64的中文字符串的表格创建
        {
            String chineseString = "中国";
            String bigChineseString = "";
            for (int i = 0; i < 10; i++){
                bigChineseString += chineseString;
            }
            PrimaryKeyValue pointBigChineseString = PrimaryKeyValue.fromString(bigChineseString);

            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString("中国"),
                    pointBigChineseString,
                    PrimaryKeyValue.fromString("法国"),
                    PrimaryKeyValue.fromString("英国")
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePoints = getPoints();
            assertEquals(points, tablePoints);
            deleteTable();
        }

        //G、预分区含有多个中英文混合字符串的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString("A中国"),
                    PrimaryKeyValue.fromString("B法国"),
                    PrimaryKeyValue.fromString("C英国")
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePointsG = getPoints();
            assertEquals(points, tablePointsG);
            deleteTable();
        }

        //H、预分区分裂点含有多个英文字符串的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString(""),
                    PrimaryKeyValue.fromString("England"),
                    PrimaryKeyValue.fromString("France ")
            );

            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);

            try {
                client.createTableEx(request, null).get();
                fail();
            } catch (TableStoreException e) {
                assertEquals("The value of partition range can not be empty string.", e.getMessage());

            }
        }

        //I、是否可以设置DataSize大于64的英文分裂点
        {
            char[] biggerChar;
            Arrays.fill(biggerChar = new char[65], 'Z');
            String biggerString = String.valueOf(biggerChar);
            PrimaryKeyValue pointBiggerString = PrimaryKeyValue.fromString(biggerString);

            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString("Chinese"),
                    PrimaryKeyValue.fromString("England"),
                    PrimaryKeyValue.fromString("France "),
                    pointBiggerString
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);

            try {
                client.createTableEx(request, null).get();
                fail();
            } catch (TableStoreException e) {
                assertEquals(String.format("The value of partition range exceeds the MaxLength:64 with CurrentLength:%d.",pointBiggerString.getDataSize()),
                        e.getMessage());
            }
        }

        //J、是否可以设置DataSize大于64的中文分裂点
        {
            String chineseString = "中国";
            String biggerChineseString = "";
            for (int i = 0; i < 11; i++){
                biggerChineseString += chineseString;
            }
            PrimaryKeyValue pointBiggerChineseString = PrimaryKeyValue.fromString(biggerChineseString);

            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromString("中国"),
                    pointBiggerChineseString,
                    PrimaryKeyValue.fromString("法国"),
                    PrimaryKeyValue.fromString("英国")
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaStr, tableOptions);
            request.setSplitPoints(points);

            try {
                client.createTableEx(request, null).get();
                fail();
            } catch (TableStoreException e) {
                assertEquals(String.format("The value of partition range exceeds the MaxLength:64 with CurrentLength:%d.",pointBiggerChineseString.getDataSize()),
                        e.getMessage());
            }
        }
    }

    @Test
    public void testPartitionBinaryPrimaryKey() throws Exception {

        deleteTable();

        TableMeta tableMetaBin = new TableMeta(tableName);
        tableMetaBin.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.BINARY));

        int timeToLive = -1;
        int maxVersions = 3;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        //A、预分区分裂点只有单个bite[]的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromBinary(new byte[]{1, 1, 1})
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaBin, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePoints = getPoints();
            assertEquals(points, tablePoints);
            deleteTable();
        }

        //B、预分区分裂点含有多个bite[]的表格创建
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromBinary(new byte[]{1, 1, 1}),
                    PrimaryKeyValue.fromBinary(new byte[]{50, 50, 50}),
                    PrimaryKeyValue.fromBinary(new byte[]{100, 100, 100})
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaBin, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePoints = getPoints();
            assertEquals(points, tablePoints);
            deleteTable();
        }

        //C、预分区分裂点含有DataSize=64的bite[]的表格创建
        {
            byte[] bigBinary;
            Arrays.fill(bigBinary = new byte[64], (byte) 1);
            PrimaryKeyValue pointBigBinary = PrimaryKeyValue.fromBinary(bigBinary);

            List<PrimaryKeyValue> points = Arrays.asList(
                    pointBigBinary,
                    PrimaryKeyValue.fromBinary(new byte[]{50, 50, 50}),
                    PrimaryKeyValue.fromBinary(new byte[]{100, 100, 100})
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaBin, tableOptions);
            request.setSplitPoints(points);
            client.createTableEx(request, null).get();

            List<PrimaryKeyValue> tablePointsC = getPoints();
            assertEquals(points, tablePointsC);
            deleteTable();
        }

        //D、是否可以设置空bite[]分裂点（不可以）**
        {
            List<PrimaryKeyValue> points = Arrays.asList(
                    PrimaryKeyValue.fromBinary(new byte[]{}),
                    PrimaryKeyValue.fromBinary(new byte[]{50, 50, 50}),
                    PrimaryKeyValue.fromBinary(new byte[]{100, 100, 100})
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaBin, tableOptions);
            request.setSplitPoints(points);

            try {
                client.createTableEx(request, null).get();
                fail();
            } catch (TableStoreException e) {
                assertEquals("The value of partition range can not be empty binary.", e.getMessage());
            }
        }

        //E、是否可以设置DataSize大于64的bite[]分裂点
        {
            byte[] biggerBinary;
            Arrays.fill(biggerBinary = new byte[65], (byte) 1);
            PrimaryKeyValue pointBiggerBinary = PrimaryKeyValue.fromBinary(biggerBinary);

            List<PrimaryKeyValue> points = Arrays.asList(
                    pointBiggerBinary,
                    PrimaryKeyValue.fromBinary(new byte[]{50, 50, 50}),
                    PrimaryKeyValue.fromBinary(new byte[]{100, 100, 100})
            );
            CreateTableRequestEx request = new CreateTableRequestEx(tableMetaBin, tableOptions);
            request.setSplitPoints(points);
            try {
                client.createTableEx(request, null).get();
                fail();
            } catch (TableStoreException e) {
                assertEquals(String.format("The value of partition range exceeds the MaxLength:64 with CurrentLength:%d.",pointBiggerBinary.getDataSize()),
                        e.getMessage());
            }
        }
    }
}
