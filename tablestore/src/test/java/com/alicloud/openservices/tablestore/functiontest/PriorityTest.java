package com.alicloud.openservices.tablestore.functiontest;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ErrorCode;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.common.ServiceSettings;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.alicloud.openservices.tablestore.core.utils.LogUtil.LOG;
import static org.junit.Assert.*;

public class PriorityTest {
    String tableName = "test_priority";

    SyncClient client = null;

    @Before
    public void setUp() {
        ServiceSettings settings = ServiceSettings.load();

        client = new SyncClient(
                settings.getOTSEndpoint(),
                settings.getOTSAccessKeyId(),
                settings.getOTSAccessKeySecret(),
                settings.getOTSInstanceName());

        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("pk", PrimaryKeyType.INTEGER));

        int timeToLive = -1;
        int maxVersions = 1;

        TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

        try {
            client.createTable(request);
        } catch (TableStoreException e) {
            if (!ErrorCode.OBJECT_ALREADY_EXIST.equals(e.getErrorCode())) {
                throw e;
            }
        }
    }

    class GetRowThread extends Thread {
        Priority priority;
        GetRowThread(Priority priority)
        {
            this.priority = priority;
        }
        String tableName = "test_priority";
        public volatile boolean exit = false;
        public volatile int successCount = 0;
        @Override
        public void run() {
            SyncClientInterface ots = Utils.getOTSInstance();
            while (!exit) {
                List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
                try {
                    pk.add(new PrimaryKeyColumn("pk", Utils.getPKColumnValue(PrimaryKeyType.INTEGER, Long.toString(this.priority.getValue()))));
                } catch (UnsupportedEncodingException e) {

                }

                Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
                columns.put("attr", ColumnValue.fromString("hello world"));

                LOG.info("put");
                try {
                    SingleRowQueryCriteria ctriteria = new SingleRowQueryCriteria(tableName, new PrimaryKey(pk));
                    ctriteria.setMaxVersions(Integer.MAX_VALUE);
                    RequestExtension extension = new RequestExtension();
                    extension.setPriority(this.priority);
                    extension.setTag("dogexxxxxxxxxxxxxxxxxxxxxxxxx" + Long.toString(this.priority.getValue()));
                    OTSHelper.getRow(ots, ctriteria, extension);

                    successCount++;
                    //System.out.println(Integer.toString(this.priority));
                } catch (TableStoreException e) {
                    //System.out.println(e.toString() + Long.toString(this.priority.toLong()));
                    LOG.info(e.toString() + Long.toString(this.priority.getValue()));
                }
            }
        }
    }

    @Test
    @Ignore("unable to config flow control by SDK")
    public void TestGetRow() throws InterruptedException {
        GetRowThread threadLow = new GetRowThread(Priority.LOW);
        GetRowThread threadHigh = new GetRowThread(Priority.NORMAL);

        threadLow.start();
        threadHigh.start();

        Thread.sleep(10000);

        threadLow.exit = true;
        threadHigh.exit = true;

        threadLow.join();
        threadHigh.join();

        float pro = 1 - threadLow.successCount / threadHigh.successCount;
        assertTrue(pro > 0.9);
    }

    @Test
    @Ignore("unable to config flow control by SDK")
    public void TestPutRow() throws InterruptedException {
        MyThread threadLow = new MyThread(Priority.LOW);
        MyThread threadHigh = new MyThread(Priority.NORMAL);

        threadLow.start();
        threadHigh.start();

        Thread.sleep(10000);

        threadLow.exit = true;
        threadHigh.exit = true;

        threadLow.join();
        threadHigh.join();

        float pro = 1 - threadLow.successCount / threadHigh.successCount;
        assertTrue(pro > 0.9);
    }

    class MyThread extends Thread {
        Priority priority;
        MyThread(Priority priority)
        {
            this.priority = priority;
        }
        String tableName = "test_priority";
        public volatile boolean exit = false;
        public volatile int successCount = 0;
        @Override
        public void run() {
            SyncClientInterface ots = Utils.getOTSInstance();
            while (!exit) {
                List<PrimaryKeyColumn> pk = new ArrayList<PrimaryKeyColumn>();
                try {
                    pk.add(new PrimaryKeyColumn("pk", Utils.getPKColumnValue(PrimaryKeyType.INTEGER, Long.toString(this.priority.getValue()))));
                } catch (UnsupportedEncodingException e) {

                }

                Map<String, ColumnValue> columns = new TreeMap<String, ColumnValue>();
                columns.put("attr", ColumnValue.fromString("hello world"));

                LOG.info("put");
                try {
                    RequestExtension extension = new RequestExtension();
                    extension.setPriority(this.priority);
                    extension.setTag("doge" + Long.toString(this.priority.getValue()));
                    OTSHelper.putRow(ots, tableName, new PrimaryKey(pk), columns, extension);
                    successCount++;
                } catch (TableStoreException e) {
                    LOG.info(e.toString() + Long.toString(this.priority.getValue()));
                }
            }
        }
    }
}

