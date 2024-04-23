package com.alicloud.openservices.tablestore.reader;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.TableMeta;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * 测试case
 * testReaderUtils:根据表结构，校验主键是否合法：功能正常
 */
public class ReaderUtilsTest {

    @Test
    public void testReaderUtils() {
        {
            // normal case
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(100L))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("testPK2")).build();
            TableMeta meta = new TableMeta("testTable");
            meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.STRING);
            try {
                ReaderUtils.checkTableMeta(meta, pk);
            } catch (Exception e) {
                assertNull(e);
            }
        }
        {
            // pk counts not equals
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(100L))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("testPK2")).build();
            TableMeta meta = new TableMeta("testTable");
            meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.STRING);
            meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.STRING);
            try {
                ReaderUtils.checkTableMeta(meta, pk);
                fail();
            } catch (Exception e) {
                assertEquals(e.getMessage(), "In table:testTable, the size of primaryKey:2 is not equals to that of the table meta:3.");
            }
        }
        {
            // pk type not equals
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(100L))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("testPK2")).build();
            TableMeta meta = new TableMeta("testTable");
            meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn("pk2", PrimaryKeyType.INTEGER);
            try {
                ReaderUtils.checkTableMeta(meta, pk);
                fail();
            } catch (Exception e) {
                assertEquals(e.getMessage(), "In table:testTable, primaryKey name : pk2: the type in meta [INTEGER] does not equals to that in primaryKey [STRING]");
            }
        }
        {
            // pk name not equals
            PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("pk1", PrimaryKeyValue.fromLong(100L))
                    .addPrimaryKeyColumn("pk2", PrimaryKeyValue.fromString("testPK2")).build();
            TableMeta meta = new TableMeta("testTable");
            meta.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
            meta.addPrimaryKeyColumn("pk3", PrimaryKeyType.INTEGER);
            try {
                ReaderUtils.checkTableMeta(meta, pk);
                fail();
            } catch (Exception e) {
                assertEquals(e.getMessage(), "In table:testTable, table do not contains primaryKey:pk2");
            }
        }
    }
}
