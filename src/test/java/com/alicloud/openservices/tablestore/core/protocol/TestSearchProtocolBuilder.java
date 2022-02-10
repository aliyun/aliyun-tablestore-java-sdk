package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.QueryFlowWeight;
import com.alicloud.openservices.tablestore.model.search.UpdateSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.analysis.FuzzyAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SingleWordAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SplitAnalyzerParameter;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestSearchProtocolBuilder extends BaseSearchTest {
    @Test
    public void buildFieldSchema_SingleWord() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.SingleWord)
                .setAnalyzerParameter(new SingleWordAnalyzerParameter(true, true));

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("single_word", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.SingleWordAnalyzerParameter param = Search.SingleWordAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertTrue(param.getCaseSensitive());
        assertTrue(param.getDelimitWord());
    }

    @Test
    public void buildFieldSchema_SingleWord_NoCaseSensitive() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.SingleWord)
                .setAnalyzerParameter(new SingleWordAnalyzerParameter().setDelimitWord(true));

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("single_word", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.SingleWordAnalyzerParameter param = Search.SingleWordAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertFalse(param.hasCaseSensitive());
        assertTrue(param.getDelimitWord());
    }

    @Test
    public void buildFieldSchema_SingleWord_NoDelimitWord() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.SingleWord)
                .setAnalyzerParameter(new SingleWordAnalyzerParameter().setCaseSensitive(true));

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("single_word", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.SingleWordAnalyzerParameter param = Search.SingleWordAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertFalse(param.hasDelimitWord());
        assertTrue(param.getCaseSensitive());
    }

    @Test
    public void buildFieldSchema_SingleWord_DefaultParam() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.SingleWord)
                .setAnalyzerParameter(new SingleWordAnalyzerParameter());

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("single_word", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.SingleWordAnalyzerParameter param = Search.SingleWordAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertFalse(param.hasCaseSensitive());
        assertFalse(param.hasDelimitWord());
    }

    @Test
    public void buildFieldSchema_SingleWord_NoParam() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.SingleWord);

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("single_word", pbFieldSchema.getAnalyzer());
        assertFalse(pbFieldSchema.hasAnalyzerParameter());
    }

    @Test
    public void buildFieldSchema_Split() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Split)
                .setAnalyzerParameter(new SplitAnalyzerParameter("-"));

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("split", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.SplitAnalyzerParameter param = Search.SplitAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertEquals("-", param.getDelimiter());
    }

    @Test
    public void buildFieldSchema_Split_DefaultParam() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Split)
                .setAnalyzerParameter(new SplitAnalyzerParameter());

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("split", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.SplitAnalyzerParameter param = Search.SplitAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertFalse(param.hasDelimiter());
    }

    @Test
    public void buildFieldSchema_Split_NoParam() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Split);

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("split", pbFieldSchema.getAnalyzer());
        assertFalse(pbFieldSchema.hasAnalyzerParameter());
    }

    @Test
    public void buildFieldSchema_Fuzzy() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Fuzzy)
                .setAnalyzerParameter(new FuzzyAnalyzerParameter(2, 4));

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("fuzzy", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.FuzzyAnalyzerParameter param = Search.FuzzyAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertEquals(2, param.getMinChars());
        assertEquals(4, param.getMaxChars());
    }

    @Test
    public void buildFieldSchema_Fuzzy_NoMinChars() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Fuzzy)
                .setAnalyzerParameter(new FuzzyAnalyzerParameter().setMaxChars(4));

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("fuzzy", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.FuzzyAnalyzerParameter param = Search.FuzzyAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertFalse(param.hasMinChars());
        assertEquals(4, param.getMaxChars());
    }

    @Test
    public void buildFieldSchema_Fuzzy_NoMaxChars() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Fuzzy)
                .setAnalyzerParameter(new FuzzyAnalyzerParameter().setMinChars(2));

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("fuzzy", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.FuzzyAnalyzerParameter param = Search.FuzzyAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertEquals(2, param.getMinChars());
        assertFalse(param.hasMaxChars());
    }

    @Test
    public void buildFieldSchema_Fuzzy_DefaultParam() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Fuzzy)
                .setAnalyzerParameter(new FuzzyAnalyzerParameter());

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("fuzzy", pbFieldSchema.getAnalyzer());
        assertTrue(pbFieldSchema.hasAnalyzerParameter());
        Search.FuzzyAnalyzerParameter param = Search.FuzzyAnalyzerParameter.parseFrom(pbFieldSchema.getAnalyzerParameter());
        assertFalse(param.hasMinChars());
    }

    @Test
    public void buildFieldSchema_Fuzzy_NoParam() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.Fuzzy);

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("fuzzy", pbFieldSchema.getAnalyzer());
        assertFalse(pbFieldSchema.hasAnalyzerParameter());
    }

    @Test
    public void buildFieldSchema_MinWord() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.MinWord);

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("min_word", pbFieldSchema.getAnalyzer());
        assertFalse(pbFieldSchema.hasAnalyzerParameter());
    }

    @Test
    public void buildFieldSchema_MinWord_IgnoreParam() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.MinWord)
                .setAnalyzerParameter(new SingleWordAnalyzerParameter());

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("min_word", pbFieldSchema.getAnalyzer());
        assertFalse(pbFieldSchema.hasAnalyzerParameter());
    }

    @Test
    public void buildFieldSchema_MaxWord() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.MaxWord);

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("max_word", pbFieldSchema.getAnalyzer());
        assertFalse(pbFieldSchema.hasAnalyzerParameter());
    }

    @Test
    public void buildFieldSchemaVirtualField() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.MaxWord)
                .setVirtualField(true)
                .setSourceFieldName("name1");

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertTrue(pbFieldSchema.getIsVirtualField());
        assertEquals(fieldSchema.getSourceFieldNames(), pbFieldSchema.getSourceFieldNamesList());
        assertEquals("name1", pbFieldSchema.getSourceFieldNamesList().get(0));
    }

    @Test
    public void buildFieldSchemaSourceFieldName() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.MaxWord)
                .setVirtualField(false)
                .setSourceFieldNames(Arrays.asList("name1", "n2", "f3"));

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertFalse(pbFieldSchema.getIsVirtualField());
        assertEquals(fieldSchema.getSourceFieldNames(), pbFieldSchema.getSourceFieldNamesList());
        assertEquals("name1", pbFieldSchema.getSourceFieldNamesList().get(0));
    }

    @Test
    public void buildFieldSchema_MaxWord_IgnoreParam() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzer(FieldSchema.Analyzer.MaxWord)
                .setAnalyzerParameter(new SingleWordAnalyzerParameter());

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertEquals("max_word", pbFieldSchema.getAnalyzer());
        assertFalse(pbFieldSchema.hasAnalyzerParameter());
    }

    @Test
    public void buildFieldSchema_NoAnalyzer_WithParam() throws InvalidProtocolBufferException {
        // model
        FieldSchema fieldSchema = new FieldSchema("Col_Name", FieldType.TEXT)
                .setAnalyzerParameter(new SingleWordAnalyzerParameter(true, true));

        // to pb
        Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);

        // assert
        assertFalse(pbFieldSchema.hasAnalyzer());
        assertFalse(pbFieldSchema.hasAnalyzerParameter());
    }

    @Test
    public void buildQueryFlowWeightNoIndexName() {
        QueryFlowWeight queryFlowWeight = new QueryFlowWeight(null, 100);

        try {
            SearchProtocolBuilder.buildQueryFlowWeight(queryFlowWeight);
            fail();
        } catch (ClientException e) {
            assertEquals("[query_flow_weight.index_name] must not be null", e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void buildQueryFlowWeightNoWeight() {
        QueryFlowWeight queryFlowWeight = new QueryFlowWeight("index1", null);

        try {
            SearchProtocolBuilder.buildQueryFlowWeight(queryFlowWeight);
            fail();
        } catch (ClientException e) {
            assertEquals("[query_flow_weight.weight] must not be null", e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void buildQueryFlowWeightNormal() {
        QueryFlowWeight queryFlowWeight = new QueryFlowWeight("index1", 80);

        Search.QueryFlowWeight pbQueryFlowWeight = SearchProtocolBuilder.buildQueryFlowWeight(queryFlowWeight);

        assertEquals("index1", pbQueryFlowWeight.getIndexName());
        assertEquals(80, pbQueryFlowWeight.getWeight());
    }

    private void assertClientException(UpdateSearchIndexRequest req, String errMsg) {
        try {
            SearchProtocolBuilder.buildUpdateSearchIndexRequest(req);
            fail();
        } catch (ClientException e) {
            assertEquals(errMsg, e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void buildUpdateSearchIndexRequestSwitchIndex() {
        UpdateSearchIndexRequest req = new UpdateSearchIndexRequest("table1", "index1_reindex", "index1");

        Search.UpdateSearchIndexRequest pbReq = SearchProtocolBuilder.buildUpdateSearchIndexRequest(req);

        assertEquals("table1", pbReq.getTableName());
        assertEquals("index1_reindex", pbReq.getIndexName());
        assertEquals("index1", pbReq.getSwitchIndexName());
    }

    @Test
    public void buildUpdateSearchIndexRequestSetQueryFlowWeightSizeIsOne() {
        List<QueryFlowWeight> weightList = Arrays.asList(
                new QueryFlowWeight("index1", 20)
        );

        UpdateSearchIndexRequest req = new UpdateSearchIndexRequest("table1", "index1_reindex", weightList);
        assertClientException(req, "[query_flow_weight] size must be 2");
    }

    @Test
    public void buildUpdateSearchIndexRequestSetQueryFlowWeightSizeIsThree() {
        List<QueryFlowWeight> weightList = Arrays.asList(
                new QueryFlowWeight("index1", 20),
                new QueryFlowWeight("index1_reindex", 80),
                new QueryFlowWeight("index_one_more", 100)
        );

        UpdateSearchIndexRequest req = new UpdateSearchIndexRequest("table1", "index1_reindex", weightList);
        assertClientException(req, "[query_flow_weight] size must be 2");
    }

    @Test
    public void buildUpdateSearchIndexRequestSetQueryFlowWeightNormal() {
        List<QueryFlowWeight> weightList = Arrays.asList(
                new QueryFlowWeight("index1", 20),
                new QueryFlowWeight("index1_reindex", 80)
        );

        UpdateSearchIndexRequest req = new UpdateSearchIndexRequest("table1", "index1_reindex", weightList);
        Search.UpdateSearchIndexRequest pbReq = SearchProtocolBuilder.buildUpdateSearchIndexRequest(req);

        assertEquals("table1", pbReq.getTableName());
        assertEquals("index1_reindex", pbReq.getIndexName());

        assertEquals(2, pbReq.getQueryFlowWeightCount());
        assertEquals("index1", pbReq.getQueryFlowWeight(0).getIndexName());
        assertEquals(20, pbReq.getQueryFlowWeight(0).getWeight());
        assertEquals("index1_reindex", pbReq.getQueryFlowWeight(1).getIndexName());
        assertEquals(80, pbReq.getQueryFlowWeight(1).getWeight());
    }

    @Test
    public void buildUpdateSearchIndexRequestNeither() {
        List<QueryFlowWeight> weightList = Arrays.asList();
        new UpdateSearchIndexRequest("table1", "index1_reindex", weightList);
        new UpdateSearchIndexRequest("table1", "index1_reindex");
    }

    @Test
    public void testSetTTL() {
        UpdateSearchIndexRequest req = new UpdateSearchIndexRequest("table1", "index1_reindex");
        req.setTimeToLive(1, TimeUnit.DAYS);
        Search.UpdateSearchIndexRequest pbReq = SearchProtocolBuilder.buildUpdateSearchIndexRequest(req);
        assertEquals("table1", pbReq.getTableName());
        assertEquals("index1_reindex", pbReq.getIndexName());
        assertEquals(TimeUnit.DAYS.toSeconds(1), pbReq.getTimeToLive());
    }
}
