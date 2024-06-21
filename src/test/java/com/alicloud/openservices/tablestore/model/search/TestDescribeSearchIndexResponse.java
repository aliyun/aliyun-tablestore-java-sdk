package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.core.protocol.BaseSearchTest;
import com.alicloud.openservices.tablestore.core.utils.Repeat;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.search.analysis.FuzzyAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SingleWordAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SplitAnalyzerParameter;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class TestDescribeSearchIndexResponse extends BaseSearchTest {

    @Test
    @Repeat(1)
    public void jsonizeAll() {
        Response resp = new Response("req_id1");
        DescribeSearchIndexResponse describeResp = new DescribeSearchIndexResponse(resp);

        //IndexSchema
        IndexSchema indexSchema = new IndexSchema();
        indexSchema.addFieldSchema(new FieldSchema("col_long", FieldType.LONG).setSubFieldSchemas(new ArrayList<FieldSchema>()));
        indexSchema.addFieldSchema(new FieldSchema("col_double", FieldType.DOUBLE).setSubFieldSchemas(new ArrayList<FieldSchema>()));
        indexSchema.addFieldSchema(new FieldSchema("col_boolean", FieldType.BOOLEAN).setSubFieldSchemas(new ArrayList<FieldSchema>()));
        indexSchema.addFieldSchema(new FieldSchema("col_keyword", FieldType.KEYWORD)
                .setSubFieldSchemas(new ArrayList<FieldSchema>())
                .setVirtualField(true)
                .setSourceFieldNames(Arrays.asList("n1", "f2", "f3")));
        indexSchema.addFieldSchema(new FieldSchema("col_fuzzy_keyword", FieldType.FUZZY_KEYWORD)
                .setSubFieldSchemas(new ArrayList<FieldSchema>())
                .setVirtualField(true)
                .setSourceFieldNames(Arrays.asList("n1", "f2", "f3")));
        indexSchema.addFieldSchema(new FieldSchema("col_text", FieldType.TEXT)
                .setSubFieldSchemas(new ArrayList<FieldSchema>())
                .setVirtualField(true)
                .setSourceFieldName("n1"));
        indexSchema.addFieldSchema(new FieldSchema("col_text_single", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.SingleWord)
                .setAnalyzerParameter(new SingleWordAnalyzerParameter(true,true))
                .setVirtualField(true)
                .setSourceFieldName("n1"));
        indexSchema.addFieldSchema(new FieldSchema("col_text_split", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Split)
                .setAnalyzerParameter(new SplitAnalyzerParameter("|"))
                .setVirtualField(true)
                .setSourceFieldName("n1"));
        indexSchema.addFieldSchema(new FieldSchema("col_text_fuzzy", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Fuzzy)
                .setAnalyzerParameter(new FuzzyAnalyzerParameter(1, 7, false))
                .setVirtualField(true)
                .setSourceFieldName("n1"));
        indexSchema.addFieldSchema(new FieldSchema("col_geo_point", FieldType.GEO_POINT).setSubFieldSchemas(new ArrayList<FieldSchema>()));
        indexSchema.addFieldSchema(new FieldSchema("col_nested", FieldType.NESTED)
                .setSubFieldSchemas(Arrays.asList(
                        new FieldSchema("col_inner", FieldType.TEXT).setSubFieldSchemas(new ArrayList<FieldSchema>()),
                        new FieldSchema("col_inner2", FieldType.TEXT)
                                .setSubFieldSchemas(new ArrayList<FieldSchema>())
                                .setVirtualField(true)
                                .setSourceFieldName("n1"))
                ));

        IndexSetting indexSetting = new IndexSetting();
        indexSetting.setRoutingFields(Arrays.asList("routing_field1", "routing_field2"));
        indexSchema.setIndexSetting(indexSetting);

        describeResp.setSchema(indexSchema);
        describeResp.setTimeToLive(1000);

        //SyncStat
        SyncStat syncStat = new SyncStat();
        syncStat.setSyncPhase(SyncStat.SyncPhase.INCR);
        syncStat.setCurrentSyncTimestamp(123456L);
        describeResp.setSyncStat(syncStat);

        describeResp.setBrotherIndexName("index1");

        describeResp.setQueryFlowWeight(Arrays.asList(
                new QueryFlowWeight("index1_reindex", 20),
                new QueryFlowWeight("index1_reindex_inner", 80)
        ));

        String json = describeResp.jsonize();
        JsonElement jsonElement = new JsonParser().parse(json);
        assertNotNull(jsonElement);
        assertEquals("{\n" +
                "  \"IndexStatus\": null,\n" +
                "  \"IndexSchema\": {\n" +
                "    \"IndexSetting\": {\"RoutingFields\": [\"routing_field1\", \"routing_field2\"]},\n" +
                "    \"FieldSchemas\": [{\n" +
                "     \"FieldName\": \"col_long\",\n" +
                "     \"FieldType\": \"LONG\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": []\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_double\",\n" +
                "     \"FieldType\": \"DOUBLE\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": []\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_boolean\",\n" +
                "     \"FieldType\": \"BOOLEAN\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": []\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_keyword\",\n" +
                "     \"FieldType\": \"KEYWORD\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": [],\n" +
                "     \"IsVirtualField\": true,\n" +
                "     \"SourceFieldNames\": [\"n1\", \"f2\", \"f3\"]\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_fuzzy_keyword\",\n" +
                "     \"FieldType\": \"FUZZY_KEYWORD\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": [],\n" +
                "     \"IsVirtualField\": true,\n" +
                "     \"SourceFieldNames\": [\"n1\", \"f2\", \"f3\"]\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_text\",\n" +
                "     \"FieldType\": \"TEXT\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": [],\n" +
                "     \"IsVirtualField\": true,\n" +
                "     \"SourceFieldNames\": [\"n1\"]\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_text_single\",\n" +
                "     \"FieldType\": \"TEXT\",\n" +
                "     \"Index\": true,\n" +
                "     \"Analyzer\": \"single_word\",\n" +
                "     \"AnalyzerParameter\": {\"CaseSensitive\": true, \"DelimitWord\": true},\n" +
                "     \"SubFieldSchemas\": [],\n" +
                "     \"IsVirtualField\": true,\n" +
                "     \"SourceFieldNames\": [\"n1\"]\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_text_split\",\n" +
                "     \"FieldType\": \"TEXT\",\n" +
                "     \"Index\": true,\n" +
                "     \"Analyzer\": \"split\",\n" +
                "     \"AnalyzerParameter\": {\"Delimiter\": \"|\", \"CaseSensitive\": null},\n" +
                "     \"SubFieldSchemas\": [],\n" +
                "     \"IsVirtualField\": true,\n" +
                "     \"SourceFieldNames\": [\"n1\"]\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_text_fuzzy\",\n" +
                "     \"FieldType\": \"TEXT\",\n" +
                "     \"Index\": true,\n" +
                "     \"Analyzer\": \"fuzzy\",\n" +
                "     \"AnalyzerParameter\": {\"MinChars\": 1, \"MaxChars\": 7, \"CaseSensitive\": false},\n" +
                "     \"SubFieldSchemas\": [],\n" +
                "     \"IsVirtualField\": true,\n" +
                "     \"SourceFieldNames\": [\"n1\"]\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_geo_point\",\n" +
                "     \"FieldType\": \"GEO_POINT\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": []\n" +
                "     },\n" +
                "     {\n" +
                "     \"FieldName\": \"col_nested\",\n" +
                "     \"FieldType\": \"NESTED\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": [{\n" +
                "     \"FieldName\": \"col_inner\",\n" +
                "     \"FieldType\": \"TEXT\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": []\n" +
                "     }, \n" +
                "     {\n" +
                "     \"FieldName\": \"col_inner2\",\n" +
                "     \"FieldType\": \"TEXT\",\n" +
                "     \"Index\": true,\n" +
                "     \"SubFieldSchemas\": [],\n" +
                "     \"IsVirtualField\": true,\n" +
                "     \"SourceFieldNames\": [\"n1\"]\n" +
                "     }]\n" +
                "     }]\n" +
                "  },\n" +
                "  \"SyncStat\": {\n" +
                "    \"SyncPhase\": \"INCR\",\n" +
                "    \"CurrentSyncTimestamp\": 123456},\n" +
                "  \"BrotherIndexName\": \"index1\",\n" +
                "  \"QueryFlowWeight\": [{\n" +
                "   \"IndexName\": \"index1_reindex\",\n" +
                "   \"Weight\": 20\n" +
                "   },\n" +
                "   {\n" +
                "   \"IndexName\": \"index1_reindex_inner\",\n" +
                "   \"Weight\": 80\n" +
                "   }],\n" +
                "  \"TimeToLive\": 1000\n" +
                "}", json);
    }
}
