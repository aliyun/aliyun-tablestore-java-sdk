package com.alicloud.openservices.tablestore.timeline.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.timeline.*;
import com.alicloud.openservices.tablestore.timeline.core.TimelineStoreFactoryImpl;
import com.alicloud.openservices.tablestore.timeline.model.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestExceptionOfServiceFactory {

    @BeforeClass
    public static void setUp() throws Exception {
    }

    @AfterClass
    public static void after() throws Exception {
    }


    @Test
    public void testServiceFactoryException() {
        SyncClient client = null;
        TimelineStoreFactory factory = null;
        try {
            factory = new TimelineStoreFactoryImpl(client);
            fail();
        } catch (TimelineException e) {
            assertEquals("SyncClient should not be null.", e.getMessage());
        }

        client = new SyncClient("http://endpoint", "accessKeyId", "accessKeySecret", "instanceName");
        try {
            factory = new TimelineStoreFactoryImpl(client);
        } catch (TimelineException e) {
            fail();
        }

        try {
            factory.createMetaStore(null);
            fail();
        } catch (TimelineException e) {
            assertEquals("TimelineMetaSchema should not be null.", e.getMessage());
        }

        try {
            factory.createTimelineStore(null);
            fail();
        } catch (TimelineException e) {
            assertEquals("TimelineSchema should not be null.", e.getMessage());
        }

        TimelineIdentifierSchema identifierSchema = new TimelineIdentifierSchema.Builder()
                .addStringField("timelineId")
                .addLongField("long")
                .build();

        try {
            factory.createMetaStore(new TimelineMetaSchema("metaTable", identifierSchema));
        } catch (TimelineException e) {
            fail();
        }

        try {
            factory.createTimelineStore(new TimelineSchema("timelineTable", identifierSchema));
        } catch (TimelineException e) {
            fail();
        }
    }
}
