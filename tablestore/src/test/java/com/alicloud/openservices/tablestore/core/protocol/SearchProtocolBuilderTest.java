package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.JsonType;
import com.alicloud.openservices.tablestore.model.search.QueryFlowWeight;
import com.alicloud.openservices.tablestore.model.search.UpdateSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.analysis.FuzzyAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SingleWordAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SplitAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.filter.SearchFilter;
import com.alicloud.openservices.tablestore.model.search.highlight.Highlight;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightEncoder;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightFragmentOrder;
import com.alicloud.openservices.tablestore.model.search.highlight.HighlightParameter;
import com.alicloud.openservices.tablestore.model.search.query.InnerHits;
import com.alicloud.openservices.tablestore.model.search.query.QueryBuilders;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.sort.DocSort;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.ScoreSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.aliyun.ots.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SearchProtocolBuilderTest extends BaseSearchTest {

    @Test
    public void buildFieldSchema_FieldType() throws InvalidProtocolBufferException {
        Map<FieldType, Search.FieldType> modelToPbFieldType = new HashMap<>();
        modelToPbFieldType.put(FieldType.LONG, Search.FieldType.LONG);
        modelToPbFieldType.put(FieldType.DOUBLE, Search.FieldType.DOUBLE);
        modelToPbFieldType.put(FieldType.BOOLEAN, Search.FieldType.BOOLEAN);
        modelToPbFieldType.put(FieldType.KEYWORD, Search.FieldType.KEYWORD);
        modelToPbFieldType.put(FieldType.TEXT, Search.FieldType.TEXT);
        modelToPbFieldType.put(FieldType.NESTED, Search.FieldType.NESTED);
        modelToPbFieldType.put(FieldType.GEO_POINT, Search.FieldType.GEO_POINT);
        modelToPbFieldType.put(FieldType.DATE, Search.FieldType.DATE);
        modelToPbFieldType.put(FieldType.VECTOR, Search.FieldType.VECTOR);
        modelToPbFieldType.put(FieldType.FUZZY_KEYWORD, Search.FieldType.FUZZY_KEYWORD);
        modelToPbFieldType.put(FieldType.IP, Search.FieldType.IP);
        modelToPbFieldType.put(FieldType.JSON, Search.FieldType.JSON);
        modelToPbFieldType.put(FieldType.FLATTENED, Search.FieldType.FLATTENED);

        for (Map.Entry<FieldType, Search.FieldType> entry : modelToPbFieldType.entrySet()) {
            FieldType modelFieldType = entry.getKey();
            Search.FieldType pbFieldType = entry.getValue();

            FieldSchema fieldSchema = new FieldSchema("Col_Name", modelFieldType);
            Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);
            assertEquals(pbFieldType, pbFieldSchema.getFieldType());
        }
    }

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
    public void buildInnerHits() {
        InnerHits.Builder innerHitsBuilder = InnerHits.newBuilder()
                .limit(10)
                .offset(0)
                .sort(new Sort(Arrays.asList(
                        new DocSort(),
                        new ScoreSort())))
                .highlight(Highlight.newBuilder()
                        .highlightEncoder(HighlightEncoder.HTML)
                        .addFieldHighlightParam("col1", HighlightParameter.newBuilder()
                                .highlightFragmentOrder(HighlightFragmentOrder.SCORE)
                                .fragmentSize(100)
                                .numberOfFragments(50)
                                .preTag("<em>")
                                .postTag("</em>")
                                .build())
                        .build());
        Search.InnerHits pbInnerHits = SearchInnerHitsBuilder.buildInnerHits(innerHitsBuilder.build());
        assertEquals(0, pbInnerHits.getOffset());
        assertEquals(10, pbInnerHits.getLimit());
        assertEquals(2, pbInnerHits.getSort().getSorterCount());
        assertTrue(pbInnerHits.getSort().getSorterList().get(0).hasDocSort());
        assertTrue(pbInnerHits.getSort().getSorterList().get(1).hasScoreSort());
        assertNotNull(pbInnerHits.getHighlight());

        Search.Highlight pbHighlight = pbInnerHits.getHighlight();
        assertEquals(Search.HighlightEncoder.HTML_MODE, pbHighlight.getHighlightEncoder());
        assertEquals(1, pbHighlight.getHighlightParametersCount());
        assertEquals(Search.HighlightFragmentOrder.SCORE, pbHighlight.getHighlightParameters(0).getFragmentsOrder());
        assertEquals(100, pbHighlight.getHighlightParameters(0).getFragmentSize());
        assertEquals(50, pbHighlight.getHighlightParameters(0).getNumberOfFragments());
        assertEquals("<em>", pbHighlight.getHighlightParameters(0).getPreTag());
        assertEquals("</em>", pbHighlight.getHighlightParameters(0).getPostTag());
    }

    @Test
    public void buildHighlight() {
        Highlight.Builder highlightBuilder = Highlight.newBuilder()
                .highlightEncoder(HighlightEncoder.HTML)
                .addFieldHighlightParam("col_name", HighlightParameter.newBuilder()
                        .highlightFragmentOrder(HighlightFragmentOrder.SCORE)
                        .fragmentSize(100)
                        .numberOfFragments(50)
                        .preTag("<em>")
                        .postTag("</em>").build());
        Search.Highlight pbHighlight = SearchHighlightBuilder.buildHighlight(highlightBuilder.build());
        assertEquals(Search.HighlightEncoder.HTML_MODE, pbHighlight.getHighlightEncoder());
        assertEquals(1, pbHighlight.getHighlightParametersCount());
        assertEquals(Search.HighlightFragmentOrder.SCORE, pbHighlight.getHighlightParameters(0).getFragmentsOrder());
        assertEquals(100, pbHighlight.getHighlightParameters(0).getFragmentSize());
        assertEquals(50, pbHighlight.getHighlightParameters(0).getNumberOfFragments());
        assertEquals("<em>", pbHighlight.getHighlightParameters(0).getPreTag());
        assertEquals("</em>", pbHighlight.getHighlightParameters(0).getPostTag());

        highlightBuilder = Highlight.newBuilder()
                .highlightEncoder(null)
                .addFieldHighlightParam(null, null);
        pbHighlight = SearchHighlightBuilder.buildHighlight(highlightBuilder.build());
        assertFalse(pbHighlight.hasHighlightEncoder());
        assertEquals(1, pbHighlight.getHighlightParametersList().size());
        assertFalse(pbHighlight.getHighlightParameters(0).hasFieldName());
        assertFalse(pbHighlight.getHighlightParameters(0).hasPreTag());
        assertFalse(pbHighlight.getHighlightParameters(0).hasPostTag());
        assertFalse(pbHighlight.getHighlightParameters(0).hasFragmentSize());
        assertFalse(pbHighlight.getHighlightParameters(0).hasNumberOfFragments());
        assertFalse(pbHighlight.getHighlightParameters(0).hasFragmentsOrder());
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
    public void testBuildSort() {
        {
            Sort sort = new Sort(Collections.singletonList(new FieldSort("field1", SortOrder.ASC)));
            Search.Sort pb = SearchSortBuilder.buildSort(sort);
            assertEquals(1, pb.getSorterCount());
            assertFalse(pb.hasDisableDefaultPkSorter());
        }
        {
            Sort sort = new Sort(Collections.singletonList(new FieldSort("field1", SortOrder.ASC)), false);
            Search.Sort pb = SearchSortBuilder.buildSort(sort);
            assertEquals(1, pb.getSorterCount());
            assertTrue(pb.hasDisableDefaultPkSorter());
            assertFalse(pb.getDisableDefaultPkSorter());
        }
        {
            Sort sort = new Sort(Collections.singletonList(new FieldSort("field1", SortOrder.ASC)), true);
            Search.Sort pb = SearchSortBuilder.buildSort(sort);
            assertEquals(1, pb.getSorterCount());
            assertTrue(pb.hasDisableDefaultPkSorter());
            assertTrue(pb.getDisableDefaultPkSorter());
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
    public void buildUpdateSearchIndexRequestAddFieldSchemas() {
        UpdateSearchIndexRequest req = new UpdateSearchIndexRequest("table1", "index1");
        List<FieldSchema> schemas = new ArrayList<>(2);
        schemas.add(new FieldSchema("new_field1", FieldType.KEYWORD));
        schemas.add(new FieldSchema("new_field2", FieldType.TEXT));
        req.setAddedFieldSchemas(schemas);

        Search.UpdateSearchIndexRequest pbReq = SearchProtocolBuilder.buildUpdateSearchIndexRequest(req);

        assertEquals("table1", pbReq.getTableName());
        assertEquals("index1", pbReq.getIndexName());
        assertNotNull(req.getAddedFieldSchemas());
        assertEquals(2, req.getAddedFieldSchemas().size());
        for (FieldSchema schema : req.getAddedFieldSchemas()) {
            if (schema.getFieldName().equals("new_field1")) {
                assertEquals(FieldType.KEYWORD, schema.getFieldType());
            }
            if (schema.getFieldName().equals("new_field2")) {
                assertEquals(FieldType.TEXT, schema.getFieldType());
            }
        }
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

    @Test
    public void testDescribeSearchIndexRequsetWithIncludeSyncStat() {
        {
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest();
            describeSearchIndexRequest.setIndexName("testIndex");
            describeSearchIndexRequest.setTableName("testTable");
            describeSearchIndexRequest.setIncludeSyncStat(false);
            Search.DescribeSearchIndexRequest pbRequest = SearchProtocolBuilder.buildDescribeSearchIndexRequest(describeSearchIndexRequest);
            assertTrue(pbRequest.hasIncludeSyncStat());
            assertFalse(pbRequest.getIncludeSyncStat());
        }
        {
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest();
            describeSearchIndexRequest.setIndexName("testIndex");
            describeSearchIndexRequest.setTableName("testTable");
            describeSearchIndexRequest.setIncludeSyncStat(true);
            Search.DescribeSearchIndexRequest pbRequest = SearchProtocolBuilder.buildDescribeSearchIndexRequest(describeSearchIndexRequest);
            assertTrue(pbRequest.hasIncludeSyncStat());
            assertTrue(pbRequest.getIncludeSyncStat());
        }
        {
            DescribeSearchIndexRequest describeSearchIndexRequest = new DescribeSearchIndexRequest();
            describeSearchIndexRequest.setIndexName("testIndex");
            describeSearchIndexRequest.setTableName("testTable");
            Search.DescribeSearchIndexRequest pbRequest = SearchProtocolBuilder.buildDescribeSearchIndexRequest(describeSearchIndexRequest);
            assertTrue(pbRequest.hasIncludeSyncStat());
            assertTrue(pbRequest.getIncludeSyncStat());
        }
    }

    @Test
    public void testBuildFilter() {
        // using query
        RangeQuery rangeQuery = new RangeQuery();
        rangeQuery.setFieldName("field1");
        rangeQuery.setFrom(ColumnValue.fromLong(1));
        rangeQuery.setTo(ColumnValue.fromLong(10));
        SearchFilter filter = SearchFilter.newBuilder().query(rangeQuery).build();
        Search.SearchFilter pbFilter = SearchFilterBuilder.buildSearchFilter(filter);
        assertTrue(pbFilter.hasQuery());
        assertEquals(Search.QueryType.RANGE_QUERY, pbFilter.getQuery().getType());

        // using query builder
        filter = SearchFilter.newBuilder().query(QueryBuilders.range("field1").greaterThanOrEqual(1).lessThanOrEqual(10)).build();
        pbFilter = SearchFilterBuilder.buildSearchFilter(filter);
        assertTrue(pbFilter.hasQuery());
        assertEquals(Search.QueryType.RANGE_QUERY, pbFilter.getQuery().getType());
    }

    @Test
    public void testBuildFieldSchema() {
        // JSON_OBJECT
        {
            FieldSchema fieldSchema =
                new FieldSchema("field1", FieldType.JSON).setJsonType(JsonType.OBJECT).setSubFieldSchemas(Collections.singletonList(new FieldSchema("subField1", FieldType.TEXT)));
            Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);
            assertEquals(Search.FieldType.JSON, pbFieldSchema.getFieldType());
            assertEquals(Search.JsonType.OBJECT_JSON, pbFieldSchema.getJsonType());
            assertEquals(1, pbFieldSchema.getFieldSchemasCount());
            assertEquals(Search.FieldType.TEXT, pbFieldSchema.getFieldSchemas(0).getFieldType());
        }
        // JSON_NESTED
        {
            FieldSchema fieldSchema =
                new FieldSchema("field1", FieldType.JSON).setJsonType(JsonType.NESTED).setSubFieldSchemas(Collections.singletonList(new FieldSchema("subField1", FieldType.TEXT)));
            Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);
            assertEquals(Search.FieldType.JSON, pbFieldSchema.getFieldType());
            assertEquals(Search.JsonType.NESTED_JSON, pbFieldSchema.getJsonType());
            assertEquals(1, pbFieldSchema.getFieldSchemasCount());
            assertEquals(Search.FieldType.TEXT, pbFieldSchema.getFieldSchemas(0).getFieldType());
        }
        // FLATTENED
        {
            FieldSchema fieldSchema =
                    new FieldSchema("field1", FieldType.FLATTENED);
            Search.FieldSchema pbFieldSchema = SearchProtocolBuilder.buildFieldSchema(fieldSchema);
            assertEquals(Search.FieldType.FLATTENED, pbFieldSchema.getFieldType());
        }
    }
}
