package com.aliyun.openservices.ots.model;

import static org.junit.Assert.*;

import org.junit.Test;

import com.aliyun.openservices.ots.model.PrimaryKeyType;
import com.aliyun.openservices.ots.model.TableMeta;

public class TableMetaTest {

    @Test
    public void testTableMetaCtor() {
        String tableName = "table";
        TableMeta tm = new TableMeta(tableName);
        assertTrue(tm.getPrimaryKey() != null);
        assertEquals(tableName, tm.getTableName());
    }

    @Test
    public void testTableMetaMembers() {
        String tableName = "table";
        TableMeta tm = new TableMeta(tableName);
        tm.addPrimaryKeyColumn("pk1", PrimaryKeyType.INTEGER);
        tm.addPrimaryKeyColumn("pk2", PrimaryKeyType.STRING);
        assertEquals(2, tm.getPrimaryKey().size());
        assertEquals(PrimaryKeyType.INTEGER, tm.getPrimaryKey().get("pk1"));
    }
}
