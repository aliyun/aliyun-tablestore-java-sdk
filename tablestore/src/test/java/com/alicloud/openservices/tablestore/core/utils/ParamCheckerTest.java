package com.alicloud.openservices.tablestore.core.utils;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ParamCheckerTest {
    private String tableName = "testTable";

    WriterConfig getWriterConfig() {
        WriterConfig config = new WriterConfig();
        config.setMaxAttrColumnSize(1024);
        config.setMaxBatchSize(4 * 1024);
        config.setMaxColumnsCount(10);
        config.setMaxPKColumnSize(1024);

        return config;
    }

    TableMeta getTableMeta() {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
        tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.BINARY);

        return tableMeta;
    }

    String getString(int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    @Test
    public void testTableNameInvalid() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abc"))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                .build();
        RowPutChange rowChange = new RowPutChange("testTable1", primaryKey);
        rowChange.addColumn("col", ColumnValue.fromLong(1));

        try {
            ParamChecker.checkRowChange(getTableMeta(), rowChange, getWriterConfig());
            fail("Expect exception happened.");
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The row to write belongs to another table.");
        }
    }

    @Test
    public void testRowSizeExceed() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abc"))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                .build();
        RowPutChange rowChange = new RowPutChange(tableName, primaryKey);

        rowChange.addColumn("col0", ColumnValue.fromString(getString(1024)));
        rowChange.addColumn("col1", ColumnValue.fromString(getString(1024)));
        rowChange.addColumn("col2", ColumnValue.fromString(getString(1024)));
        rowChange.addColumn("col3", ColumnValue.fromString(getString(1024)));

        try {
            ParamChecker.checkRowChange(getTableMeta(), rowChange, getWriterConfig());
            fail("Expect exception happened.");
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The row size exceeds the max batch size: 4096.");
        }
    }

    @Test
    public void testInvalidPrimaryKeySchema() {
        {
            // test auto increment
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.BINARY);
            tableMeta.addPrimaryKeyColumn("pk3", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);

            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abc"))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                    .addPrimaryKeyColumn("pk3", PrimaryKeyValue.AUTO_INCREMENT)
                    .build();

            RowPutChange rowChange = new RowPutChange(tableName, primaryKey);

            rowChange.addColumn("col0", ColumnValue.fromString(getString(100)));

            ParamChecker.checkRowChange(tableMeta, rowChange, getWriterConfig());
        }
        {
            // test auto increment
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.BINARY);
            tableMeta.addPrimaryKeyColumn("pk3", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);

            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abc"))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                    .addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromLong(100))
                    .build();

            RowPutChange rowChange = new RowPutChange(tableName, primaryKey);

            rowChange.addColumn("col0", ColumnValue.fromString(getString(100)));

            ParamChecker.checkRowChange(tableMeta, rowChange, getWriterConfig());
        }
        {
            // test auto increment
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("pk0", PrimaryKeyType.INTEGER);
            tableMeta.addPrimaryKeyColumn("pk1", PrimaryKeyType.STRING);
            tableMeta.addPrimaryKeyColumn("pk2", PrimaryKeyType.BINARY);
            tableMeta.addPrimaryKeyColumn("pk3", PrimaryKeyType.INTEGER, PrimaryKeyOption.AUTO_INCREMENT);

            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abc"))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                    .addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromString(""))
                    .build();

            RowPutChange rowChange = new RowPutChange(tableName, primaryKey);

            rowChange.addColumn("col0", ColumnValue.fromString(getString(100)));

            try {
                ParamChecker.checkRowChange(tableMeta, rowChange, getWriterConfig());
                fail("Expect exception happened.");
            } catch (ClientException e) {
                assertEquals(e.getMessage(), "The type of primary key column 'pk3' is STRING, but it's defined as INTEGER in table meta.");
            }
        }

        {
            // pk count not equal
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abc"))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                    .addPrimaryKeyColumn("pk3", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                    .build();
            RowPutChange rowChange = new RowPutChange(tableName, primaryKey);

            rowChange.addColumn("col0", ColumnValue.fromString(getString(100)));

            try {
                ParamChecker.checkRowChange(getTableMeta(), rowChange, getWriterConfig());
                fail("Expect exception happened.");
            } catch (ClientException e) {
                assertEquals(e.getMessage(), "The primary key schema is not match which defined in table meta.");
            }
        }

        {
            // invalid primary key column schema
            PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString("abc"))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("cde"))
                    .build();
            RowPutChange rowChange = new RowPutChange(tableName, primaryKey);

            rowChange.addColumn("col0", ColumnValue.fromString(getString(100)));

            try {
                ParamChecker.checkRowChange(getTableMeta(), rowChange, getWriterConfig());
                fail("Expect exception happened.");
            } catch (ClientException e) {
                assertEquals(e.getMessage(), "The type of primary key column 'pk2' is STRING, but it's defined as BINARY in table meta.");
            }
        }
    }

    @Test
    public void testPrimaryKeyColumnSizeExceed() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(getString(1025)))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                .build();
        RowPutChange rowChange = new RowPutChange(tableName, primaryKey);

        rowChange.addColumn("col0", ColumnValue.fromString(getString(100)));

        try {
            ParamChecker.checkRowChange(getTableMeta(), rowChange, getWriterConfig());
            fail("Expect exception happened.");
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The size of primary key column 'pk1' has exceeded the max length:1024.");
        }
    }

    @Test
    public void testColumnCountExceed() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(getString(5)))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                .build();
        RowPutChange rowChange = new RowPutChange(tableName, primaryKey);
        for (int i = 0; i < getWriterConfig().getMaxColumnsCount() + 1; i++) {
            rowChange.addColumn("col" + i, ColumnValue.fromLong(i));
        }

        try {
            ParamChecker.checkRowChange(getTableMeta(), rowChange, getWriterConfig());
            fail("Expect exception happened.");
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The count of attribute columns exceeds the maximum: 10.");
        }
    }

    @Test
    public void testColumnNameEqualWithPrimaryKeyColumn() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(getString(5)))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                .build();
        RowPutChange rowChange = new RowPutChange(tableName, primaryKey);
        rowChange.addColumn("pk0", ColumnValue.fromLong(0));

        try {
            ParamChecker.checkRowChange(getTableMeta(), rowChange, getWriterConfig());
            fail("Expect exception happened.");
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The attribute column's name duplicate with primary key column, which is 'pk0'.");
        }
    }

    @Test
    public void testColumnSizeExceed() {
        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromLong(0))
                .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(getString(5)))
                .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromBinary(new byte[]{0x0, 0x1, 0x2}))
                .build();
        RowPutChange rowChange = new RowPutChange(tableName, primaryKey);
        rowChange.addColumn("col0", ColumnValue.fromString(getString(getWriterConfig().getMaxAttrColumnSize() + 1)));

        try {
            ParamChecker.checkRowChange(getTableMeta(), rowChange, getWriterConfig());
            fail("Expect exception happened.");
        } catch (ClientException e) {
            assertEquals(e.getMessage(), "The size of attribute column 'col0' has exceeded the max length: 1024.");
        }
    }
}
