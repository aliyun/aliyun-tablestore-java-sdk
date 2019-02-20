package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.fail;

public class TestDeleteTableRequest {

    @Test
    public void testInvalidArguments() {
        try {
            DeleteTableRequest request = new DeleteTableRequest(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            DeleteTableRequest request = new DeleteTableRequest("T");
            request.setTableName(null);
            fail();
        } catch (IllegalArgumentException e) {
            
        }
    }
}
