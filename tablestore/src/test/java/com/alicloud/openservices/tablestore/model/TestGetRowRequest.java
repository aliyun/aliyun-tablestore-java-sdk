package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.fail;

public class TestGetRowRequest {

    @Test
    public void testInvalidArguments() {
        try {
            GetRowRequest request = new GetRowRequest();
            request.setRowQueryCriteria(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
