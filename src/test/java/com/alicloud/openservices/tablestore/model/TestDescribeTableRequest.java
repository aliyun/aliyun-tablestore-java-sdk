package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.fail;

public class TestDescribeTableRequest {

    @Test
    public void testInvalidArguments() {
        try {
            DescribeTableRequest request = new DescribeTableRequest(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
        try {
            DescribeTableRequest request = new DescribeTableRequest("T");
            request.setTableName(null);
            fail();
        } catch (IllegalArgumentException e) {
            
        }
    }
}
