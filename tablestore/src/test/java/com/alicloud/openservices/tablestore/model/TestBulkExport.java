package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBulkExport {

    static String tableName = "YSTestBulkExport";

    @Test
    public void testBulkExportQueryCriteria() {
        BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);

        PrimaryKey primaryKey = new PrimaryKey();
        try {
            bulkExportQueryCriteria.setInclusiveStartPrimaryKey(primaryKey);
        } catch (IllegalArgumentException e) {
            assertEquals("The inclusive start primary key should not be null.", e.getMessage());
        }
        try {
            bulkExportQueryCriteria.setExclusiveEndPrimaryKey(primaryKey);
        } catch (IllegalArgumentException e) {
            assertEquals("The exclusive end primary key should not be null.", e.getMessage());
        }

        DataBlockType dataBlockType = DataBlockType.DBT_SIMPLE_ROW_MATRIX;
        bulkExportQueryCriteria.setDataBlockType(dataBlockType);
        assertEquals(DataBlockType.DBT_SIMPLE_ROW_MATRIX, bulkExportQueryCriteria.getDataBlockType());


    }

    @Test
    public void testBulkExportRequest() {
        BulkExportQueryCriteria bulkExportQueryCriteria = new BulkExportQueryCriteria(tableName);
        BulkExportRequest bulkExportRequest = new BulkExportRequest();
        bulkExportRequest.setBulkExportQueryCriteria(bulkExportQueryCriteria);

    }
}
