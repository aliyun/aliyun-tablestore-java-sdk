package com.alicloud.openservices.tablestore.core.http;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestOTSUri {

    @Test
    public void testInvalidEndpoint() {
        try {
            OTSUri uri = new OTSUri("invalid uri", "ListTable");
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testValidEndpoint() {
        // not with '/' in the end
        OTSUri uri = new OTSUri("http://test-java-sdk.ots.aliyuncs.com", "ListTable");
        assertEquals(uri.getUri().toString(), "http://test-java-sdk.ots.aliyuncs.com/ListTable");
        assertEquals(uri.getHost().toHostString(), "test-java-sdk.ots.aliyuncs.com");
        assertEquals(uri.getAction(), "ListTable");

        // with '/' in the end
        uri = new OTSUri("http://test-java-sdk.ots.aliyuncs.com/", "GetRow");
        assertEquals(uri.getUri().toString(), "http://test-java-sdk.ots.aliyuncs.com/GetRow");
        assertEquals(uri.getHost().toHostString(), "test-java-sdk.ots.aliyuncs.com");
        assertEquals(uri.getAction(), "GetRow");

        // with more than one '/' in the end
        uri = new OTSUri("http://test-java-sdk.ots.aliyuncs.com/////", "UpdateRow");
        assertEquals(uri.getUri().toString(), "http://test-java-sdk.ots.aliyuncs.com/UpdateRow");
        assertEquals(uri.getHost().toHostString(), "test-java-sdk.ots.aliyuncs.com");
        assertEquals(uri.getAction(), "UpdateRow");
    }
}
