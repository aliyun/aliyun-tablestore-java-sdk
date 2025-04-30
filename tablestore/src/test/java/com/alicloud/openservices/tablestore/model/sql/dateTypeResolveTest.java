package com.alicloud.openservices.tablestore.model.sql;

import com.alicloud.openservices.tablestore.core.protocol.sql.flatbuffers.SQLResponseColumns;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.*;

import static org.junit.Assert.assertEquals;

public class dateTypeResolveTest {
    @Test
    public void testResolveDateTime() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        SQLResponseColumns columns = new SQLResponseColumns();
        SQLRowsFBsColumnBased sqlRowsFBsColumnBased = new SQLRowsFBsColumnBased(columns);
        Method privateMethod = SQLRowsFBsColumnBased.class.getDeclaredMethod("resolveDateTime",long.class);
        privateMethod.setAccessible(true);

        // Test 0: Unix timestamp = 1689840111874921
        long unixTimestamp = 1689840111874921L;
        ZonedDateTime expectedDateTime = ZonedDateTime.of(2023, 7, 20, 8, 1, 51, 874921000, ZoneOffset.UTC);
        ZonedDateTime actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);
        
        // Test 1: Unix timestamp = 0
        unixTimestamp = 0;
        expectedDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);

        // Test 2: Unix timestamp = 1
        unixTimestamp = 1;
        expectedDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 1000, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);

        // Test 3: Unix timestamp = 999
        unixTimestamp = 999;
        expectedDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 999000, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);

        // Test 4: Unix timestamp = 1000
        unixTimestamp = 1000000;
        expectedDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 1, 0, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);

        // Test 5: Unix timestamp = 31536000000000 (1 year)
        unixTimestamp = 31536000000000L;
        expectedDateTime = ZonedDateTime.of(1971, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);

        // Test 6: Unix timestamp = 946080000000000 (30 years)
        unixTimestamp = 946080000000000L;
        expectedDateTime = ZonedDateTime.of(1999, 12, 25, 0, 0, 0, 0, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);

        // Test 7: Unix timestamp = 32503680000000000 (1000 years)
        unixTimestamp = 32503680000000000L;
        expectedDateTime = ZonedDateTime.of(3000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);

        // Test 8: zUnix timestamp = 253402300799999000
        unixTimestamp = 253402300799999000L;
        expectedDateTime = ZonedDateTime.of(9999, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);

        // Test 9: Unix timestamp = -1000
        unixTimestamp = -1000;
        expectedDateTime = ZonedDateTime.of(1969, 12, 31, 23, 59, 59, 999000000, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);

        // Test 10: Unix timestamp = -999000
        unixTimestamp = -999000;
        expectedDateTime = ZonedDateTime.of(1969, 12, 31, 23, 59, 59, 1000000, ZoneOffset.UTC);
        actualDateTime = getDatetime(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDateTime, actualDateTime);
    }

    @Test
    public void testResolveTime() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        SQLResponseColumns columns = new SQLResponseColumns();
        SQLRowsFBsColumnBased sqlRowsFBsColumnBased = new SQLRowsFBsColumnBased(columns);
        Method privateMethod = SQLRowsFBsColumnBased.class.getDeclaredMethod("resolveTime", long.class);
        privateMethod.setAccessible(true);

        // Test 1: 1 nanosecond
        long val = 1;
        Duration expectedDuration = Duration.ofNanos(1);
        Duration actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 2: 999 nanoseconds
        val = 999;
        expectedDuration = Duration.ofNanos(999);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 3: 1 microsecond
        val = 1000;
        expectedDuration = Duration.ofNanos(1000);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 4: 999 microseconds
        val = 999000;
        expectedDuration = Duration.ofNanos(999000);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 5: 1 millisecond
        val = 1000000;
        expectedDuration = Duration.ofNanos(1000000);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 6: 999 milliseconds
        val = 999000000;
        expectedDuration = Duration.ofNanos(999000000);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 7: 1 second
        val = 1000000000;
        expectedDuration = Duration.ofSeconds(1);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 8: 59 seconds and 999,999,999 nanoseconds
        val = 59999999999L;
        expectedDuration = Duration.ofSeconds(59).plusNanos(999999999);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 9: 1 minute
        val = 60000000000L;
        expectedDuration = Duration.ofMinutes(1);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 10: 23 hours, 59 minutes, 59 seconds, and 999,999,999 nanoseconds
        val = 86399999999999L;
        expectedDuration = Duration.ofHours(23)
                .plusMinutes(59)
                .plusSeconds(59)
                .plusNanos(999999999);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

        // Test 10: 375 hours, 59 minutes, 59 seconds, and 999,999,999 nanoseconds
        val = 1353599999999999L;
        expectedDuration = Duration.ofHours(375)
                .plusMinutes(59)
                .plusSeconds(59)
                .plusNanos(999999999);
        actualDuration = getTime(sqlRowsFBsColumnBased,privateMethod,val);
        assertEquals(expectedDuration, actualDuration);

    }

    @Test
    public void testResolveDate() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        SQLResponseColumns columns = new SQLResponseColumns();
        SQLRowsFBsColumnBased sqlRowsFBsColumnBased = new SQLRowsFBsColumnBased(columns);
        Method privateMethod = SQLRowsFBsColumnBased.class.getDeclaredMethod("resolveDate",long.class);
        privateMethod.setAccessible(true);
        // Test 0: Unix timestamp = 1689840111874921
        long unixTimestamp = 1689840111L;
        LocalDate expectedDate = LocalDate.of(2023, 7, 20);
        LocalDate actualDate = getDate(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDate, actualDate);

        // Test 1: Unix timestamp = 0
        unixTimestamp = 0;
        expectedDate = LocalDate.of(1970, 1, 1);
        actualDate = getDate(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDate, actualDate);

        // Test 2: Unix timestamp = 31536000 (1 year)
        unixTimestamp = 31536000L;
        expectedDate = LocalDate.of(1971, 1, 1);
        actualDate = getDate(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDate, actualDate);

        // Test 3: Unix timestamp = 946080000 (30 years)
        unixTimestamp = 946080000L;
        expectedDate = LocalDate.of(1999, 12, 25);
        actualDate = getDate(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDate, actualDate);

        // Test 4: Unix timestamp = 32503680000 (1000 years)
        unixTimestamp = 32503680000L;
        expectedDate = LocalDate.of(3000, 1, 1);
        actualDate = getDate(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDate, actualDate);

        // Test 5: Unix timestamp = 253402300799
        unixTimestamp = 253402300799L;
        expectedDate = LocalDate.of(9999, 12, 31);
        actualDate = getDate(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDate, actualDate);

        // Test 6: Unix timestamp = -1000
        unixTimestamp = -1000;
        expectedDate = LocalDate.of(1969, 12, 31);
        actualDate = getDate(sqlRowsFBsColumnBased,privateMethod,unixTimestamp);
        assertEquals(expectedDate, actualDate);
    }

    private ZonedDateTime getDatetime(SQLRowsFBsColumnBased sqlRowsFBsColumnBased,Method privateMethod,long val) throws InvocationTargetException, IllegalAccessException {
        return (ZonedDateTime) privateMethod.invoke(sqlRowsFBsColumnBased,val);
    }

    private Duration getTime(SQLRowsFBsColumnBased sqlRowsFBsColumnBased, Method privateMethod, long val) throws InvocationTargetException, IllegalAccessException {
        return (Duration) privateMethod.invoke(sqlRowsFBsColumnBased,val);
    }

    private LocalDate getDate(SQLRowsFBsColumnBased sqlRowsFBsColumnBased, Method privateMethod, long val) throws InvocationTargetException, IllegalAccessException {
        return (LocalDate) privateMethod.invoke(sqlRowsFBsColumnBased,val);
    }
}
