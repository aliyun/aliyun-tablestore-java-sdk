package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.fail;

public class TestCreateTableRequest {

    @Test
    public void testInvalidArguments() {
        TableMeta tableMeta = new TableMeta("TableName");
        tableMeta.addPrimaryKeyColumn("PK1", PrimaryKeyType.INTEGER);
        CreateTableRequest request =
            new CreateTableRequest(tableMeta, new TableOptions());
        try {
            request.setTableMeta(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            request.setTableOptions(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            request.setReservedThroughput(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }

    @Test
    public void testInvalidArgumentsEx() {
        TableMeta tableMeta = new TableMeta("TableName");
        tableMeta.addPrimaryKeyColumn("PK1", PrimaryKeyType.INTEGER);
        CreateTableRequest request =
                new CreateTableRequest(tableMeta, new TableOptions());
        try {
            request.setTableMeta(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            request.setTableOptions(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            request.setReservedThroughput(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
