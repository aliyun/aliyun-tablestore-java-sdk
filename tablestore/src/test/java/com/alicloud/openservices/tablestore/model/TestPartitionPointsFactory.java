package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @Author wtt
 * @create 2021/7/29 5:06 PM
 */
public class TestPartitionPointsFactory {

    @Test
    public void testSplitPointFactory(){

        // Get the numeric split point
        {
            List<PrimaryKeyValue> points = SplitPointFactory.getDigit(4, 0,100);
            List<PrimaryKeyValue> pointsTrue = Arrays.asList(
                    PrimaryKeyValue.fromLong(20),
                    PrimaryKeyValue.fromLong(40),
                    PrimaryKeyValue.fromLong(60),
                    PrimaryKeyValue.fromLong(80)
            );
            assertEquals(pointsTrue, points);
        }

        // Get the numeric split point
        {
            List<PrimaryKeyValue> points = SplitPointFactory.getDigit(3, 2,5);
            List<PrimaryKeyValue> pointsTrue = Arrays.asList(
                    PrimaryKeyValue.fromLong(3),
                    PrimaryKeyValue.fromLong(4),
                    PrimaryKeyValue.fromLong(5)
            );
            assertEquals(pointsTrue, points);
        }

        // Get 4 split points in the range [0,5] --> [-INF,1),[1,2),[2,3),[3,4),[4,+INF)
        {
            List<PrimaryKeyValue> points = SplitPointFactory.getDigit(4, 0,5);
            List<PrimaryKeyValue> pointsTrue = Arrays.asList(
                    PrimaryKeyValue.fromLong(1),
                    PrimaryKeyValue.fromLong(2),
                    PrimaryKeyValue.fromLong(3),
                    PrimaryKeyValue.fromLong(4)
            );
            assertEquals(pointsTrue, points);
        }

        // Obtain 5 split points in the range [0,5] (6 intervals). The range is too small, so an exception occurs.
        {
            try {
                List<PrimaryKeyValue> points = SplitPointFactory.getDigit(6, 0, 5);
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals("When you try to set the split points for the interval [0, 5], you can set up to 5 split points, currently 6.", e.getMessage());
            }
        }


        // Obtain 3 split points in the range [0,10] --> [-INF,2), [2,5), [5,8), [8,+INF)
        {
            List<PrimaryKeyValue> points = SplitPointFactory.getDigit(3, 0,10);
            List<PrimaryKeyValue> pointsTrue = Arrays.asList(
                    PrimaryKeyValue.fromLong(2),
                    PrimaryKeyValue.fromLong(5),
                    PrimaryKeyValue.fromLong(8)
            );
            assertEquals(pointsTrue, points);
        }

        // Get the numeric split points, the expected number of split points is 0.
        {
            try {
                List<PrimaryKeyValue> points = SplitPointFactory.getDigit(0, 0, 100);
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals("The number of pointCount must be greater than 0.", e.getMessage());
            }
        }

        // Get a lowercase MD5 hash of length 1 --> [+INF,4),[4,8),[8,c),[c,-INF)
        {
            List<PrimaryKeyValue> points = SplitPointFactory.getLowerHexString(3,1);
            List<PrimaryKeyValue> pointsTrue = Arrays.asList(
                    PrimaryKeyValue.fromString("4"),
                    PrimaryKeyValue.fromString("8"),
                    PrimaryKeyValue.fromString("c")
            );
            assertEquals(pointsTrue, points);
        }

        // Get a lowercase MD5 hash of length 1, finest granularity partitioning
        {
            List<PrimaryKeyValue> points = SplitPointFactory.getLowerHexString(15,1);
            List<PrimaryKeyValue> pointsTrue = Arrays.asList(
                    PrimaryKeyValue.fromString("1"),
                    PrimaryKeyValue.fromString("2"),
                    PrimaryKeyValue.fromString("3"),
                    PrimaryKeyValue.fromString("4"),
                    PrimaryKeyValue.fromString("5"),
                    PrimaryKeyValue.fromString("6"),
                    PrimaryKeyValue.fromString("7"),
                    PrimaryKeyValue.fromString("8"),
                    PrimaryKeyValue.fromString("9"),
                    PrimaryKeyValue.fromString("a"),
                    PrimaryKeyValue.fromString("b"),
                    PrimaryKeyValue.fromString("c"),
                    PrimaryKeyValue.fromString("d"),
                    PrimaryKeyValue.fromString("e"),
                    PrimaryKeyValue.fromString("f")
            );
            assertEquals(pointsTrue, points);
        }


        // Get the lowercase MD5 hash with a length of 1
        {
            List<PrimaryKeyValue> points = SplitPointFactory.getLowerHexString(4,4);
            List<PrimaryKeyValue> pointsTrue = Arrays.asList(
                    PrimaryKeyValue.fromString("3333"),
                    PrimaryKeyValue.fromString("6666"),
                    PrimaryKeyValue.fromString("9999"),
                    PrimaryKeyValue.fromString("cccc")
            );
            assertEquals(pointsTrue, points);
        }

        // Get the lowercase MD5 split point, with the expected number of split points being 0.
        {
            try {
                List<PrimaryKeyValue> points = SplitPointFactory.getLowerHexString(0, 4);
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals("The number of pointCount and pointLength must be greater than 0.", e.getMessage());
            }
        }

        // Get the lowercase MD5 split point, expecting the MD5 length to be 0.
        {
            try {
                List<PrimaryKeyValue> points = SplitPointFactory.getLowerHexString(16, 1);
                fail();
            } catch (IllegalArgumentException e) {
                assertEquals("When the length of MD5 is 1, you can set up to 15 split points, currently 16.", e.getMessage());
            }
        }

        // Get the uppercase MD5 code with a length of 1
        {
            List<PrimaryKeyValue> points = SplitPointFactory.getUpperHexString(4,4);
            List<PrimaryKeyValue> pointsTrue = Arrays.asList(
                    PrimaryKeyValue.fromString("3333"),
                    PrimaryKeyValue.fromString("6666"),
                    PrimaryKeyValue.fromString("9999"),
                    PrimaryKeyValue.fromString("CCCC")
            );
            assertEquals(pointsTrue, points);
        }
    }

    @Test
    public void testSetPoints() {
        List<PrimaryKeyValue> pointsNull = null;
        List<PrimaryKeyValue> pointsNullList = new ArrayList<PrimaryKeyValue>();

        try {
            CreateTableRequestEx request = new CreateTableRequestEx(new TableMeta("TableName"), new TableOptions());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("TableMeta should set at least one primary key.", e.getMessage());
        }

        TableMeta tableMetaInt = new TableMeta("TABLE_NAME");
        tableMetaInt.addPrimaryKeyColumn(new PrimaryKeySchema("PK", PrimaryKeyType.INTEGER));
        CreateTableRequestEx requestInt = new CreateTableRequestEx(tableMetaInt, new TableOptions());

        List<PrimaryKeyValue> pointsInt = Arrays.asList(
                PrimaryKeyValue.fromLong(10000),
                PrimaryKeyValue.fromLong(20000),
                PrimaryKeyValue.fromLong(30000)
        );
        requestInt.setSplitPoints(pointsInt);
        assertEquals(requestInt.getSplitPoints(), pointsInt);

        try {
            requestInt.setSplitPoints(pointsNull);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point list cannot be empty.", e.getMessage());
        }

        try {
            requestInt.setSplitPoints(pointsNullList);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point list cannot be empty.", e.getMessage());
        }

        try {
            requestInt.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromLong(10000),
                    PrimaryKeyValue.INF_MIN,
                    PrimaryKeyValue.fromLong(30000)
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point can't be set as an INF value.", e.getMessage());
        }

        try {
            requestInt.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromLong(10000),
                    PrimaryKeyValue.INF_MAX,
                    PrimaryKeyValue.fromLong(30000)
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point can't be set as an INF value.", e.getMessage());
        }

        try {
            requestInt.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromLong(20000),
                    PrimaryKeyValue.fromLong(10000),
                    PrimaryKeyValue.fromLong(30000)
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point list isn't strictly increasing.", e.getMessage());
        }

        try {
            requestInt.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromLong(10000),
                    PrimaryKeyValue.fromString("fff"),
                    PrimaryKeyValue.fromBinary(new byte[]{100, 100, 100})
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point's type doesn't match the partition key's type.", e.getMessage());
        }


        TableMeta tableMetaStr = new TableMeta("TABLE_NAME");
        tableMetaStr.addPrimaryKeyColumn(new PrimaryKeySchema("PK", PrimaryKeyType.STRING));
        CreateTableRequestEx requestStr = new CreateTableRequestEx(tableMetaStr, new TableOptions());

        List<PrimaryKeyValue> pointsStrEnglish = Arrays.asList(
                PrimaryKeyValue.fromString("aaa"),
                PrimaryKeyValue.fromString("fff"),
                PrimaryKeyValue.fromString("zzz")
        );
        requestStr.setSplitPoints(pointsStrEnglish);
        assertEquals(requestStr.getSplitPoints(), pointsStrEnglish);

        List<PrimaryKeyValue> pointsStrChinese = Arrays.asList(
                PrimaryKeyValue.fromString("中国"),
                PrimaryKeyValue.fromString("法国")
        );
        requestStr.setSplitPoints(pointsStrChinese);
        assertEquals(requestStr.getSplitPoints(), pointsStrChinese);

        try {
            requestStr.setSplitPoints(pointsNull);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point list cannot be empty.", e.getMessage());
        }

        try {
            requestStr.setSplitPoints(pointsNullList);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point list cannot be empty.", e.getMessage());
        }

        try {
            requestStr.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromString("aaa"),
                    PrimaryKeyValue.INF_MIN,
                    PrimaryKeyValue.fromString("zzz")
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point can't be set as an INF value.", e.getMessage());
        }

        try {
            requestStr.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromString("aaa"),
                    PrimaryKeyValue.INF_MAX,
                    PrimaryKeyValue.fromString("zzz")
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point can't be set as an INF value.", e.getMessage());
        }

        try {
            requestStr.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromString("fff"),
                    PrimaryKeyValue.fromString("aaa"),
                    PrimaryKeyValue.fromString("zzz")
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point list isn't strictly increasing.", e.getMessage());
        }

        try {
            requestStr.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromLong(10000),
                    PrimaryKeyValue.fromString("fff"),
                    PrimaryKeyValue.fromBinary(new byte[]{100, 100, 100})
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point's type doesn't match the partition key's type.", e.getMessage());
        }

        TableMeta tableMetaBin = new TableMeta("TABLE_NAME");
        tableMetaBin.addPrimaryKeyColumn(new PrimaryKeySchema("PK", PrimaryKeyType.BINARY));
        CreateTableRequestEx requestBin = new CreateTableRequestEx(tableMetaBin, new TableOptions());

        List<PrimaryKeyValue> pointsBin = Arrays.asList(
                PrimaryKeyValue.fromBinary(new byte[]{1,1,1}),
                PrimaryKeyValue.fromBinary(new byte[]{50,50,50}),
                PrimaryKeyValue.fromBinary(new byte[]{100,100,100})
        );
        requestBin.setSplitPoints(pointsBin);
        assertEquals(pointsBin, requestBin.getSplitPoints());

        try {
            requestBin.setSplitPoints(pointsNull);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point list cannot be empty.", e.getMessage());
        }


        try {
            requestBin.setSplitPoints(pointsNullList);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point list cannot be empty.", e.getMessage());
        }


        try {
            requestBin.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromBinary(new byte[]{1,1,1}),
                    PrimaryKeyValue.INF_MIN,
                    PrimaryKeyValue.fromBinary(new byte[]{100,100,100})
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point can't be set as an INF value.", e.getMessage());
        }

        try {
            requestBin.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromBinary(new byte[]{1,1,1}),
                    PrimaryKeyValue.INF_MAX,
                    PrimaryKeyValue.fromBinary(new byte[]{100,100,100})
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point can't be set as an INF value.", e.getMessage());
        }

        try {
            requestBin.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromBinary(new byte[]{50, 50, 50}),
                    PrimaryKeyValue.fromBinary(new byte[]{1, 1 ,1}),
                    PrimaryKeyValue.fromBinary(new byte[]{100,100,100})
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point list isn't strictly increasing.", e.getMessage());
        }

        try {
            requestBin.setSplitPoints(Arrays.asList(
                    PrimaryKeyValue.fromLong(10000),
                    PrimaryKeyValue.fromString("fff"),
                    PrimaryKeyValue.fromBinary(new byte[]{100, 100, 100})
            ));
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("The split-point's type doesn't match the partition key's type.", e.getMessage());
        }

    }

}
