package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.timestream.TimestreamDBConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TimestreamDBConfigurationUnittest {

    @Test
    public void testBasic() {
        String metaTableName = "metaTable";
        TimestreamDBConfiguration conf = new TimestreamDBConfiguration(metaTableName);
        conf.setMaxDataTableNumForWrite(5);
        conf.setIntervalDumpMeta(1, TimeUnit.MINUTES);
        conf.setMetaCacheSize(4 * 1024 * 1024);
        conf.setThreadNumForWriter(10);
        conf.setDumpMeta(false);
        Assert.assertEquals(conf.getMetaTableName(), metaTableName);
        Assert.assertFalse(conf.getDumpMeta());
        Assert.assertEquals(conf.getMaxDataTableNumForWrite(), 5);
        Assert.assertEquals(conf.getIntervalDumpMeta(TimeUnit.SECONDS), 1 * 60);
        Assert.assertEquals(conf.getMetaCacheSize(), 4 * 1024 * 1024);
        Assert.assertEquals(conf.getThreadNumForWriter(), 10);
    }

    @Test
    public void testInvalidMetaName() {
        String metaName = null;
        try {
            new TimestreamDBConfiguration(metaName);
            Assert.fail();
        } catch (ClientException e) {
            // pass
        }
        metaName = "";
        try {
            new TimestreamDBConfiguration(metaName);
            Assert.fail();
        } catch (ClientException e) {
            // pass
        }
    }
}
