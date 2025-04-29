package com.alicloud.openservices.tablestore.timeline.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.timeline.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline.TimelineStore;
import com.alicloud.openservices.tablestore.timeline.TimelineStoreFactory;
import com.alicloud.openservices.tablestore.timeline.common.Conf;
import com.alicloud.openservices.tablestore.timeline.core.TimelineStoreFactoryImpl;
import com.alicloud.openservices.tablestore.timeline.model.*;
import com.alicloud.openservices.tablestore.timeline.query.SearchResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Arrays;

import static com.alicloud.openservices.tablestore.timeline.common.ServiceWrapper.sleepForSyncData;
import static org.junit.Assert.assertEquals;

public class TestSearchQueue {
    private static String metaTableName = "metaTable";
    private static String metaIndexName = "metaIndex";
    private static String dataTableName = "dataTable";
    private static String dataIndexName = "dataIndex";
    private static String sequenceIdName = "dataIndex";


    private static TimelineIdentifierSchema identifierSchema = new TimelineIdentifierSchema.Builder()
            .addStringField("timelineId")
            .build();

    private static SyncClient client;
    private static TimelineStoreFactory factory = null;
    private static TimelineMetaStore metaService = null;
    private static TimelineStore timelineStore = null;

    @BeforeClass
    public static void setUp() throws Exception {
        try {
            System.out.println("start before class.");
            Conf conf = Conf.newInstance(System.getProperty("user.home") + "/tablestoreConf.json");
            client = new SyncClient(conf.getEndpoint(), conf.getAccessId(), conf.getAccessKey(), conf.getInstanceName());
            factory = new TimelineStoreFactoryImpl(client);

            try {
                initMetaTable();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                initDataTable();
            } catch (Exception e) {
                e.printStackTrace();
            }
            insertMeta();
            insertData();

            System.out.println("sleep in before class.");

            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("finish before class.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void after() {
        timelineStore.flush();

        metaService.dropAllTables();
        timelineStore.dropAllTables();

        client.shutdown();
    }

    @Test
    public void testNotSearchTimeline() {

        System.out.println("start test function.");
        MatchPhraseQuery query2 = new MatchPhraseQuery();
        query2.setFieldName("text");
        query2.setText("ots2");

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query2);
        searchQuery.setTrackTotalCount(SearchQuery.TRACK_TOTAL_COUNT);
        searchQuery.setLimit(100);

        TimelineQueue timelineQueue = timelineStore.createTimelineQueue(new TimelineIdentifier.Builder()
                .addField("timelineId", "group_2")
                .build());


        SearchResult<TimelineEntry> result = timelineQueue.search(searchQuery);
        assertEquals(2, result.getTotalCount());
    }

    /**
     * init meta table service
     * */
    private static void initMetaTable() {
        IndexSchema metaIndex = new IndexSchema();
        metaIndex.addFieldSchema(new FieldSchema("groupName", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord));
        metaIndex.addFieldSchema(new FieldSchema("createTime", FieldType.LONG).setIndex(true));
        metaIndex.addFieldSchema(new FieldSchema("location", FieldType.GEO_POINT).setIndex(true));
        metaIndex.addFieldSchema(new FieldSchema("isPublic", FieldType.BOOLEAN).setIndex(true));
        metaIndex.addFieldSchema(new FieldSchema("point", FieldType.DOUBLE).setIndex(true));
        metaIndex.addFieldSchema(new FieldSchema("tags", FieldType.KEYWORD).setIndex(true).setIsArray(true));

        TimelineMetaSchema metaSchema = new TimelineMetaSchema(metaTableName, identifierSchema)
                .withIndex(metaIndexName, metaIndex);
        metaService = factory.createMetaStore(metaSchema);
        metaService.prepareTables();
    }

    /**
     * init store table service
     * */
    private static void initDataTable() {
        IndexSchema dataIndex = new IndexSchema();
        dataIndex.addFieldSchema(new FieldSchema("timelineId", FieldType.KEYWORD).setIndex(true));
        dataIndex.addFieldSchema(new FieldSchema("text", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord));
        dataIndex.addFieldSchema(new FieldSchema("receivers", FieldType.KEYWORD).setIsArray(true).setIndex(true));
        dataIndex.addFieldSchema(new FieldSchema("timestamp", FieldType.LONG).setEnableSortAndAgg(true));
        dataIndex.addFieldSchema(new FieldSchema("from", FieldType.KEYWORD));

        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setFlushInterval(1000);

        TimelineSchema dataSchema = new TimelineSchema(dataTableName, identifierSchema)
                .withIndex(dataIndexName, dataIndex)
                .autoGenerateSeqId()
                .setSequenceIdColumnName(sequenceIdName)
                .setTimeToLive(-1)
                .withWriterConfig(writerConfig);
        timelineStore = factory.createTimelineStore(dataSchema);
        timelineStore.prepareTables();
    }

    /**
     * insert meta into metaTable
     * */
    private static void insertMeta() {
        for (int i = 0; i < 10; i++) {
            TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                    .addField("timelineId", "group_" + i)
                    .build();

            TimelineMeta insertGroup = new TimelineMeta(identifier)
                    .setField("groupName", "表格存储" + i)
                    .setField("createTime", i)
                    .setField("location", "30,12" + i)
                    .setField("isPublic", i % 2 == 0)
                    .setField("point", i + 0.0D)
                    .setField(new Column("tags", ColumnValue.fromString("[\"Table\",\"Store\"]")));

            metaService.insert(insertGroup);
        }
    }

    /**
     * insert data into dataTable
     * */
    private static void insertData() {
        String[] groupMember = new String[]{"user_a", "user_b", "user_c"};


        for (int i = 0; i < 10; i++) {
            TimelineQueue groupTimelineQueue = timelineStore.createTimelineQueue(
                    new TimelineIdentifier.Builder()
                            .addField("timelineId", "group_1")
                            .build()
            );

            TimelineMessage tm = new TimelineMessage()
                    .setField("text", "hello TableStore ots" + i)
                    .setField("receivers", groupMember)
                    .setField("timestamp", i)
                    .setField("from", "ots");
            groupTimelineQueue.store(tm);
        }
        for (int i = 0; i < 10; i++) {
            TimelineQueue groupTimelineQueue = timelineStore.createTimelineQueue(
                    new TimelineIdentifier.Builder()
                            .addField("timelineId", "group_2")
                            .build()
            );

            TimelineMessage tm = new TimelineMessage()
                    .setField("text", "hello TableStore ots" + i)
                    .setField("receivers", groupMember)
                    .setField("timestamp", i)
                    .setField("from", "ots");
            groupTimelineQueue.store(tm);
        }


        for (int i = 0; i < 10; i++) {
            TimelineQueue groupTimelineQueue = timelineStore.createTimelineQueue(
                    new TimelineIdentifier.Builder()
                            .addField("timelineId", "group_2")
                            .build()
            );

            TimelineMessage tm = new TimelineMessage()
                    .setField("text", "fine TableStore ots" + i)
                    .setField("receivers", groupMember)
                    .setField("timestamp", i)
                    .setField("from", "ots");
            groupTimelineQueue.store(tm);
        }
    }
}
