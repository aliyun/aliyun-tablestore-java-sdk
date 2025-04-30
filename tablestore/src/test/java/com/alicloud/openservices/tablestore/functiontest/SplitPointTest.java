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

        // A, table creation with pre-split keys that are single digits
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

        // B, table creation with pre-split partition points containing multiple numbers
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

        //C, create multiple partitions, exceed the partition limit on the server side, and throw exceptions
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

        // D, if the pre-split shard key contains Long.MIN_VALUE, throw an exception.
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

        //E, if the pre-split shard contains Long.MAX_VALUE, an exception is thrown.
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

        // F, after setting the split point, modify the attributes of PrimaryKey through setTableMeta.
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

        // A, table creation with pre-split points having only a single English string.
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
        // B, table creation with pre-split keys containing multiple English strings
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

        // C. Create a table with pre-split shards containing an English string with DataSize=64.
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

        //D, Table creation with pre-split keys that are single Chinese character strings only.
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

        // E, table creation with pre-split keys containing multiple Chinese strings
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
        // F, Create a table with pre-split shards containing a Chinese string with DataSize=64.
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

        //G, Create a table with pre-partitioning containing multiple mixed Chinese and English strings
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

        //H, Table creation with multiple English strings in the pre-divided split points
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

        // I, can the DataSize be set to a value greater than 64 in English split points?
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

        //J, can the DataSize be set to a value greater than 64 for the Chinese split point?
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

        // A, pre-splitting points for table creation with only a single byte[]
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

        // B, the pre-split partition contains multiple bite[] for table creation
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

        // C. The pre-splitting partition point contains a bite[] with DataSize=64 for table creation.
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

        //D, Can an empty bite[] split point be set? (No)**
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

        // E, can a split point with DataSize greater than 64 be set for bite[]?
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
