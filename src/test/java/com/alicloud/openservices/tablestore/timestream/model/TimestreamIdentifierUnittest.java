package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.timestream.TimestreamRestrict;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

public class TimestreamIdentifierUnittest {

    @Test
    public void testBaisc() {
        {
            TimestreamIdentifier meta1 = new TimestreamIdentifier.Builder("mem")
                    .addTag("k1", "v1")
                    .addTag("k2", "v2")
                    .addTag("k3", "v3")
                    .addTag("k4", "v4")
                    .addTag("k5", "v5")
                    .build();

            Assert.assertEquals("mem", meta1.getName());
            Assert.assertEquals("v1", meta1.getTagValue("k1"));
            Assert.assertEquals("v2", meta1.getTagValue("k2"));
            Assert.assertEquals("v3", meta1.getTagValue("k3"));
            Assert.assertEquals("v4", meta1.getTagValue("k4"));
            Assert.assertEquals("v5", meta1.getTagValue("k5"));
        }
        {
            TimestreamIdentifier meta1 = new TimestreamIdentifier.Builder("mem")
                    .addTag("k1", "v1")
                    .addTag("k2", "v2")
                    .addTag("k3", "v3")
                    .addTag("k4", "v4")
                    .addTag("k5", "v5").build();

            TimestreamIdentifier meta2 = new TimestreamIdentifier.Builder("mem")
                    .addTag("k1", "v1")
                    .addTag("k2", "v2")
                    .addTag("k3", "v3")
                    .addTag("k4", "v4")
                    .addTag("k5", "v5")
                    .build();

            Assert.assertEquals(meta1, meta2);
        }
    }

    @Test
    public void testInvalidKeyValue() {
        {
            TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder("mem");
            try {
                builder.addTag("redchen=", "test");
                Assert.fail();
            } catch (ClientException e) {
                Assert.assertEquals("Illegal character exist: =.", e.getMessage());
            }
        }
        {
            TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder("mem");
            builder.addTag("redchen", "tes=t");
        }
    }

    @Test
    public void testNameRestrict() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < TimestreamRestrict.NAME_LEN_BYTE; ++i) {
            sb.append("a");
        }
        {
            TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder(sb.toString());
        }
        {
            sb.append("a");
            try {
                TimestreamIdentifier.Builder builder = new TimestreamIdentifier.Builder(sb.toString());
                Assert.fail();
            } catch (ClientException e) {
                // pass
            }
        }
    }
}
