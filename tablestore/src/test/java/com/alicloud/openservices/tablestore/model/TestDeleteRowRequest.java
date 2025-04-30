package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.fail;

public class TestDeleteRowRequest {

    @Test
    public void testInvalidArguments() {
        DeleteRowRequest request = new DeleteRowRequest();

        try {
            request.setRowChange(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
