package com.alicloud.openservices.tablestore.model;


import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.common.BaseFT;
import com.alicloud.openservices.tablestore.common.OTSHelper;
import com.alicloud.openservices.tablestore.common.Utils;
import com.google.gson.JsonSyntaxException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;

public class UpdateTableTest extends BaseFT {
    private static final int MILLISECONDS_UNTIL_TABLE_READY = 10 * 1000;

    private static final String tableName = "UpdateTableTest";
    private static SyncClientInterface client;
    private static Logger LOG = Logger.getLogger(BatchWriteTest.class.getName());

    @BeforeClass
    public static void classBefore() throws JsonSyntaxException, IOException {
        client = Utils.getOTSInstance();
    }

    @Before
    public void setup() throws Exception {
        OTSHelper.deleteAllTable(client);
    }

    /**
     * 通过createTable和updateTable设置原始列，通过describeTable验证原始列是否设置成功
     * @throws Exception
     */
    @Test
    public void testWithUpdateColumnToGet() throws Exception {
            TableMeta tableMeta = new TableMeta(tableName);
            tableMeta.addPrimaryKeyColumn("PK1", PrimaryKeyType.STRING);
            CreateTableRequest request = new CreateTableRequest(tableMeta, new com.alicloud.openservices.tablestore.model.TableOptions(-1, 1));
            StreamSpecification streamSpecification = new StreamSpecification(true, 168);
            String[] s = new String[]{"col1", "col2"};
            streamSpecification.addOriginColumnsToGet(s);
            request.setStreamSpecification(streamSpecification);
            client.createTable(request);
            Thread.sleep(MILLISECONDS_UNTIL_TABLE_READY);

            UpdateTableRequest updateTableRequest = new UpdateTableRequest(tableName);
            String[] s1 = new String[]{"col3", "col4"};
            streamSpecification.addOriginColumnsToGet(s1);
            updateTableRequest.setStreamSpecification(streamSpecification);
            client.updateTable(updateTableRequest);

            DescribeTableRequest describeTableRequest = new DescribeTableRequest(tableName);
            DescribeTableResponse describeTableResponse = client.describeTable(describeTableRequest);
            StreamDetails streamDetails = describeTableResponse.getStreamDetails();
            System.out.println("stream details："
                + streamDetails.toString());

            assertEquals(true, describeTableResponse.getStreamDetails().getOriginColumnsToGet().contains("col1"));
            assertEquals(true, describeTableResponse.getStreamDetails().getOriginColumnsToGet().contains("col2"));
            assertEquals(true, describeTableResponse.getStreamDetails().getOriginColumnsToGet().contains("col3"));
            assertEquals(true, describeTableResponse.getStreamDetails().getOriginColumnsToGet().contains("col4"));
            assertEquals(false, describeTableResponse.getStreamDetails().getOriginColumnsToGet().contains("col5"));
            assertEquals(4, describeTableResponse.getStreamDetails().getOriginColumnsToGet().size());
    }
}
