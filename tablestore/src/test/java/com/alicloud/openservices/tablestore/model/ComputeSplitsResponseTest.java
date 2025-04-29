package com.alicloud.openservices.tablestore.model;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.*;

public class ComputeSplitsResponseTest {

    @Test
    public void splitsSize() {
        Response meta = new Response();
        ComputeSplitsResponse computeSplitsResponse = new ComputeSplitsResponse(meta);
        computeSplitsResponse.setSplitsSize(123);
        assertEquals(123, computeSplitsResponse.getSplitsSize(),0.001);
    }

    @Test
    public void sessionId() throws UnsupportedEncodingException {
        Response meta = new Response();
        ComputeSplitsResponse computeSplitsResponse = new ComputeSplitsResponse(meta);
        computeSplitsResponse.setSessionId("123".getBytes("utf-8"));
        assertEquals(Arrays.toString("123".getBytes("utf-8")), Arrays.toString(computeSplitsResponse.getSessionId()));
    }

}