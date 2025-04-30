package com.alicloud.openservices.tablestore.model;

import org.junit.Test;

import static org.junit.Assert.fail;

public class TestGetRangeRequest {

    @Test
    public void testInvalidArguments() {
        try {
            GetRangeRequest request = new GetRangeRequest();
            request.setRangeRowQueryCriteria(null);
            fail();
        } catch (IllegalArgumentException e) {

        }
    }
}
