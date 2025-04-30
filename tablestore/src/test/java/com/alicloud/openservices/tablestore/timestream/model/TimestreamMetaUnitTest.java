package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TimestreamMetaUnitTest {

    @Test
    public void testAttribute() {
        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("").build();
        TimestreamMeta meta = new TimestreamMeta(identifier);
        meta.addAttribute("a1", "v1");
        meta.addAttribute("a2", "v2");

        Assert.assertEquals("v1", meta.getAttributeAsString("a1"));
        Assert.assertEquals("v2", meta.getAttributeAsString("a2"));
    }

    @Test
    public void testInvalidKeyValue() {
        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("mem").build();
        {
            try {
                TimestreamMeta meta = new TimestreamMeta(identifier);
                meta.addAttribute(TableMetaGenerator.CN_TAMESTAMP_NAME, "test");
                Assert.fail();
            } catch (ClientException e) {
                Assert.assertEquals("Key of attribute cannot be " + TableMetaGenerator.CN_TAMESTAMP_NAME + ".", e.getMessage());
            }
        }
        {
            TimestreamMeta meta = new TimestreamMeta(identifier);
            meta.addAttribute(TableMetaGenerator.CN_TAMESTAMP_NAME + ".", "tes=t");
        }
    }

    @Test
    public void testUpdateTime() {
        TimestreamIdentifier identifier = new TimestreamIdentifier.Builder("").build();
        TimestreamMeta meta = new TimestreamMeta(identifier);
        long ts = 123456;
        meta.setUpdateTime(ts, TimeUnit.SECONDS);
        Assert.assertEquals(TimeUnit.SECONDS.toMicros(ts), meta.getUpdateTimeInUsec());
    }
}
