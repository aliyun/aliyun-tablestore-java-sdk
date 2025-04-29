package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import org.junit.Test;

import static org.junit.Assert.*;

public class SearchVariantTypeTest {

    @Test
    public void testForceConvertToDestType() throws Exception {
        {
            Long lValue = 1234L;
            ColumnValue columnValue = SearchVariantType.forceConvertToDestColumnValue(
                SearchVariantType.fromLong(lValue));
            assertEquals(columnValue.getType(), ColumnType.INTEGER);
        }

        {
            double dValue = 123.456;
            ColumnValue columnValue = SearchVariantType.forceConvertToDestColumnValue(
                SearchVariantType.fromDouble(dValue));
            assertEquals(columnValue.getType(), ColumnType.DOUBLE);
        }

        {
            String sValue = "123.456";
            ColumnValue columnValue = SearchVariantType.forceConvertToDestColumnValue(
                SearchVariantType.fromString(sValue));
            assertEquals(columnValue.getType(), ColumnType.STRING);
        }
        {
            boolean bValue = false;
            ColumnValue columnValue = SearchVariantType.forceConvertToDestColumnValue(
                SearchVariantType.fromBoolean(bValue));
            assertEquals(columnValue.getType(), ColumnType.BOOLEAN);
        }
    }
}