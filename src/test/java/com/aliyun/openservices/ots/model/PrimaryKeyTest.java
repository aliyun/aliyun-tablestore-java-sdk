package com.aliyun.openservices.ots.model;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrimaryKeyTest {

    @Test
    public void testOperations() {
        RowPrimaryKey primaryKey = new RowPrimaryKey();
        primaryKey.addPrimaryKeyColumn("A", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("B", PrimaryKeyValue.fromLong(2))
                .addPrimaryKeyColumn("C", PrimaryKeyValue.fromLong(3));

        assertEquals(primaryKey.getSize(), 3 + 8 * 3);
    }

    @Test
    public void testEquals() {
        RowPrimaryKey primaryKey1 = new RowPrimaryKey();
        primaryKey1.addPrimaryKeyColumn("A", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("B", PrimaryKeyValue.fromLong(2))
                .addPrimaryKeyColumn("C", PrimaryKeyValue.fromLong(3))
                .addPrimaryKeyColumn("D", PrimaryKeyValue.fromLong(4))
                .addPrimaryKeyColumn("E", PrimaryKeyValue.fromLong(5));

        RowPrimaryKey primaryKey2 = new RowPrimaryKey();
        primaryKey2.addPrimaryKeyColumn("A", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("B", PrimaryKeyValue.fromLong(2))
                .addPrimaryKeyColumn("C", PrimaryKeyValue.fromLong(3))
                .addPrimaryKeyColumn("D", PrimaryKeyValue.fromLong(4))
                .addPrimaryKeyColumn("E", PrimaryKeyValue.fromLong(5));

        assertEquals(primaryKey1, primaryKey2);
        assertEquals(primaryKey1.hashCode(), primaryKey2.hashCode());

        RowPrimaryKey primaryKey3 = new RowPrimaryKey();
        primaryKey3.addPrimaryKeyColumn("A", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("B", PrimaryKeyValue.fromLong(2))
                .addPrimaryKeyColumn("F", PrimaryKeyValue.fromLong(3))
                .addPrimaryKeyColumn("D", PrimaryKeyValue.fromLong(4))
                .addPrimaryKeyColumn("E", PrimaryKeyValue.fromLong(5));

        assertTrue(!primaryKey1.equals(primaryKey3));

        RowPrimaryKey primaryKey4 = new RowPrimaryKey();
        primaryKey4.addPrimaryKeyColumn("A", PrimaryKeyValue.fromLong(1))
                .addPrimaryKeyColumn("B", PrimaryKeyValue.fromLong(2))
                .addPrimaryKeyColumn("C", PrimaryKeyValue.fromLong(3))
                .addPrimaryKeyColumn("D", PrimaryKeyValue.fromLong(4));
        assertTrue(!primaryKey1.equals(primaryKey4));
    }
}

