package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.fail;

public class TestCreateTableRequest {

    @Test
    public void testInvalidArguments() {
        CreateTableRequest request =
            new CreateTableRequest(new TableMeta("TableName"), new TableOptions());
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
        CreateTableRequest request =
                new CreateTableRequest(new TableMeta("TableName"), new TableOptions());
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
