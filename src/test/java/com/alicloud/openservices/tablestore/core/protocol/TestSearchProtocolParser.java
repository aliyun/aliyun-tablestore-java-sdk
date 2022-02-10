package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.protocol.Search.Aggregations;
import com.alicloud.openservices.tablestore.core.protocol.Search.GroupBySort;
import com.alicloud.openservices.tablestore.core.protocol.Search.GroupBys;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.QueryFlowWeight;
import com.alicloud.openservices.tablestore.model.search.ScanQuery;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.analysis.FuzzyAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SingleWordAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SplitAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupBy;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

public class TestSearchProtocolParser extends BaseSearchTest {

    @Test
    public void toFieldSchema_FieldSchemaNotSet() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertTrue(result.isIndex());
        assertNull(result.isEnableSortAndAgg());
        assertNull(result.isStore());
        assertNull(result.isArray());
    }

    @Test
    public void toFieldSchema_SingleWord() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("single_word");

        Search.SingleWordAnalyzerParameter.Builder b = Search.SingleWordAnalyzerParameter.newBuilder();
        b.setCaseSensitive(true);
        b.setDelimitWord(true);
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.SingleWord, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof SingleWordAnalyzerParameter);
        assertTrue(((SingleWordAnalyzerParameter) result.getAnalyzerParameter()).isCaseSensitive());
        assertTrue(((SingleWordAnalyzerParameter) result.getAnalyzerParameter()).isDelimitWord());
    }

    @Test
    public void toFieldSchema_SingleWord_NoCaseSensitive() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("single_word");

        Search.SingleWordAnalyzerParameter.Builder b = Search.SingleWordAnalyzerParameter.newBuilder();
        b.setDelimitWord(true);
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.SingleWord, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof SingleWordAnalyzerParameter);
        assertNull(((SingleWordAnalyzerParameter) result.getAnalyzerParameter()).isCaseSensitive());
        assertTrue(((SingleWordAnalyzerParameter) result.getAnalyzerParameter()).isDelimitWord());
    }

    @Test
    public void toFieldSchema_SingleWord_NoDelimitWord() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("single_word");

        Search.SingleWordAnalyzerParameter.Builder b = Search.SingleWordAnalyzerParameter.newBuilder();
        b.setCaseSensitive(true);
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.SingleWord, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof SingleWordAnalyzerParameter);
        assertTrue(((SingleWordAnalyzerParameter) result.getAnalyzerParameter()).isCaseSensitive());
        assertNull(((SingleWordAnalyzerParameter) result.getAnalyzerParameter()).isDelimitWord());
    }

    @Test
    public void toFieldSchema_SingleWord_NoParameter() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("single_word");

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());   // there's no fail
        assertEquals(FieldSchema.Analyzer.SingleWord, result.getAnalyzer());
        assertNull(result.getAnalyzerParameter());
    }

    @Test
    public void toFieldSchema_SingleWord_DefaultParameter() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("single_word");

        Search.SingleWordAnalyzerParameter.Builder b = Search.SingleWordAnalyzerParameter.newBuilder();
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());   // there's no fail
        assertEquals(FieldSchema.Analyzer.SingleWord, result.getAnalyzer());
        assertNotNull(result.getAnalyzerParameter());
        assertNull(((SingleWordAnalyzerParameter) result.getAnalyzerParameter()).isCaseSensitive());
        assertNull(((SingleWordAnalyzerParameter) result.getAnalyzerParameter()).isDelimitWord());
    }

    @Test
    public void toFieldSchema_Split() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("split");

        Search.SplitAnalyzerParameter.Builder b = Search.SplitAnalyzerParameter.newBuilder();
        b.setDelimiter("-");
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Split, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof SplitAnalyzerParameter);
        assertEquals("-", ((SplitAnalyzerParameter) result.getAnalyzerParameter()).getDelimiter());
    }

    @Test
    public void toFieldSchema_Split_NoParameter() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("split");

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Split, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() == null);
    }

    @Test
    public void toFieldSchema_Split_DefaultParam() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("split");

        Search.SplitAnalyzerParameter.Builder b = Search.SplitAnalyzerParameter.newBuilder();
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Split, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof SplitAnalyzerParameter);
        assertNull(((SplitAnalyzerParameter) result.getAnalyzerParameter()).getDelimiter());
    }

    @Test
    public void toFieldSchema_Fuzzy() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("fuzzy");

        Search.FuzzyAnalyzerParameter.Builder b = Search.FuzzyAnalyzerParameter.newBuilder();
        b.setMinChars(2);
        b.setMaxChars(3);
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Fuzzy, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof FuzzyAnalyzerParameter);
        assertEquals(2, ((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMinChars().intValue());
        assertEquals(3, ((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMaxChars().intValue());
    }

    @Test
    public void toFieldSchema_Fuzzy_NoMinChars() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("fuzzy");

        Search.FuzzyAnalyzerParameter.Builder b = Search.FuzzyAnalyzerParameter.newBuilder();
        b.setMaxChars(3);
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Fuzzy, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof FuzzyAnalyzerParameter);
        assertNull(((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMinChars());
        assertEquals(3, ((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMaxChars().intValue());
    }

    @Test
    public void toFieldSchema_Fuzzy_NoMaxChars() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("fuzzy");

        Search.FuzzyAnalyzerParameter.Builder b = Search.FuzzyAnalyzerParameter.newBuilder();
        b.setMinChars(1);
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Fuzzy, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof FuzzyAnalyzerParameter);
        assertEquals(1, ((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMinChars().intValue());
        assertNull(((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMaxChars());
    }

    @Test
    public void toFieldSchema_Fuzzy_NoParameter() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("fuzzy");

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Fuzzy, result.getAnalyzer());
        assertNull(result.getAnalyzerParameter());
    }

    @Test
    public void toFieldSchema_Fuzzy_DefaultParameter() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("fuzzy");

        Search.FuzzyAnalyzerParameter.Builder b = Search.FuzzyAnalyzerParameter.newBuilder();
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Fuzzy, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof FuzzyAnalyzerParameter);
        assertNull(((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMinChars());
        assertNull(((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMaxChars());
    }

    @Test
    public void toFieldSchema_MinWord() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("min_word");

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.MinWord, result.getAnalyzer());
        assertNull(result.getAnalyzerParameter());
    }

    @Test
    public void toFieldSchema_MaxWord() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("max_word");

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.MaxWord, result.getAnalyzer());
        assertNull(result.getAnalyzerParameter());
    }

    @Test
    public void toFieldSchema_VirtualField() {
        {
            Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
            builder.setIsVirtualField(true);
            builder.addSourceFieldNames("123");

            FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
            assertNotNull(result.isVirtualField());
            assertTrue(result.isVirtualField());
            List<String> sourceFieldNames = result.getSourceFieldNames();
            assertEquals(1, sourceFieldNames.size());
            assertEquals("123", sourceFieldNames.get(0));
        }
        {
            Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
            builder.setIsVirtualField(false);

            FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
            assertNotNull(result.isVirtualField());
            assertFalse(result.isVirtualField());
            List<String> sourceFieldNames = result.getSourceFieldNames();
            assertEquals(0, sourceFieldNames.size());
        }
        {
            Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
            builder.addSourceFieldNames("123");
            builder.addSourceFieldNames("456f");

            FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
            assertNull(result.isVirtualField());
            List<String> sourceFieldNames = result.getSourceFieldNames();
            assertEquals(2, sourceFieldNames.size());
            assertEquals("123", sourceFieldNames.get(0));
            assertEquals("456f", sourceFieldNames.get(1));
        }
    }

    private void assertClientException(Search.FieldSchema fieldSchema, String errMsg) {
        try {
            SearchProtocolParser.toFieldSchema(fieldSchema);
            fail();
        } catch (ClientException e) {
            assertEquals(errMsg, e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }


    @Test
    public void toFieldSchema_InvalidAnalyzer() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("invalid_analyzer");
        assertClientException(builder.build(), "Unknown analyzer");
    }

    private void assertClientException(Search.QueryFlowWeight queryFlowWeight, String errMsg) {
        try {
            SearchProtocolParser.toQueryFlowWeight(queryFlowWeight);
            fail();
        } catch (ClientException e) {
            assertEquals(errMsg, e.getMessage());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void toQueryFlowWeightNoIndexName() {
        Search.QueryFlowWeight.Builder builder = Search.QueryFlowWeight.newBuilder();
        builder.setWeight(100);
        assertClientException(builder.build(), "[query_flow_weight] has no index name");
    }

    @Test
    public void toQueryFlowWeightNoWeight() {
        Search.QueryFlowWeight.Builder builder = Search.QueryFlowWeight.newBuilder();
        builder.setIndexName("index1");
        assertClientException(builder.build(), "[query_flow_weight] has no weight");
    }

    @Test
    public void toQueryFlowWeightNormal() {
        Search.QueryFlowWeight.Builder builder = Search.QueryFlowWeight.newBuilder();
        builder.setIndexName("index1");
        builder.setWeight(80);

        QueryFlowWeight queryFlowWeight = SearchProtocolParser.toQueryFlowWeight(builder.build());

        assertEquals("index1", queryFlowWeight.getIndexName());
        assertEquals(80, queryFlowWeight.getWeight().intValue());

    }


    @Test
    public void testSortSerialization() throws IOException {
        Sort origin = randomSort();
        Search.Sort pb = SearchSortBuilder.buildSort(origin);
        Sort newObj = SearchSortParser.toSort(pb);
        assertJsonEquals(origin, newObj);
    }

    @Test
    public void testGroupBySortSerialization() throws IOException {
        List<GroupBySorter> origin = randomGroupBySorterList();
        GroupBySort pb = SearchSortBuilder.buildGroupBySort(origin);
        List<GroupBySorter> newObj = SearchSortParser.toGroupBySort(pb);
        assertJsonEquals(origin, newObj);
    }

    @Test
    public void testAggregationsSerialization() throws IOException {
        List<Aggregation> origin = randomAggregations();
        Aggregations pb = SearchAggregationBuilder.buildAggregations(origin);
        List<Aggregation> newObj = SearchAggregationParser.toAggregations(pb);
        assertJsonEquals(origin, newObj);
    }

    @Test
    public void testGroupBysSerialization() throws IOException {
        List<GroupBy> origin = randomGroupBys();
        GroupBys pb = SearchGroupByBuilder.buildGroupBys(origin);
        List<GroupBy> newObj = SearchGroupByParser.toGroupBys(pb);
        assertJsonEquals(origin, newObj);
    }

    @Test
    public void testQuerySerialization() throws IOException {
        {
            Query origin = randomQuery();
            Search.Query pb = SearchQueryBuilder.buildQuery(origin);
            Query newObj = SearchQueryParser.toQuery(pb);
            assertJsonEquals(origin, newObj);
        }
        {
            Query origin = randomQuery();
            byte[] pb = SearchQueryBuilder.buildQueryToBytes(origin);
            Query newObj = SearchQueryParser.toQuery(pb);
            assertJsonEquals(origin, newObj);
        }
    }

    @Test
    public void testSearchQuerySerialization() throws IOException {
        {
            SearchQuery origin = randomSearchQuery();
            Search.SearchQuery pb = SearchProtocolBuilder.buildSearchQuery(origin);
            SearchQuery newObj = SearchProtocolParser.toSearchQuery(pb.toByteString());
            assertJsonEquals(origin, newObj);
        }
        {
            SearchQuery origin = randomSearchQuery();
            byte[] pb = SearchProtocolBuilder.buildSearchQueryToBytes(origin);
            SearchQuery newObj = SearchProtocolParser.toSearchQuery(pb);
            assertJsonEquals(origin, newObj);
        }
    }

    @Test
    public void testSearchRequestSerialization() throws IOException {
        {
            SearchRequest origin = randomSearchRequest();
            Search.SearchRequest pb = SearchProtocolBuilder.buildSearchRequest(origin);
            SearchRequest newObj = SearchProtocolParser.toSearchRequest(pb.toByteString());
            assertJsonEquals(origin, newObj);
        }
        {
            SearchRequest origin = randomSearchRequest();
            byte[] pb = SearchProtocolBuilder.buildSearchRequestToBytes(origin);
            SearchRequest newObj = SearchProtocolParser.toSearchRequest(pb);
            assertJsonEquals(origin, newObj);
        }
    }

    @Test
    public void testScanQuerySerialization() throws IOException {
        {
            ScanQuery origin = randomScanQuery();
            Search.ScanQuery pb = SearchProtocolBuilder.buildScanQuery(origin);
            ScanQuery newObj = SearchProtocolParser.toScanQuery(pb.toByteString());
            assertJsonEquals(origin, newObj);
        }
        {
            ScanQuery origin = randomScanQuery();
            byte[] pb = SearchProtocolBuilder.buildScanQueryToBytes(origin);
            ScanQuery newObj = SearchProtocolParser.toScanQuery(pb);
            assertJsonEquals(origin, newObj);
        }
    }

    @Test
    public void testParallelScanRequestSerialization() throws IOException {
        {
            ParallelScanRequest origin = randomParallelScanRequest();
            Search.ParallelScanRequest pb = SearchProtocolBuilder.buildParallelScanRequest(origin);
            ParallelScanRequest newObj = SearchProtocolParser.toParallelScanRequest(pb.toByteString());
            assertJsonEquals(origin, newObj);
        }
        {
            ParallelScanRequest origin = randomParallelScanRequest();
            byte[] pb = SearchProtocolBuilder.buildParallelScanRequestToBytes(origin);
            ParallelScanRequest newObj = SearchProtocolParser.toParallelScanRequest(pb);
            assertJsonEquals(origin, newObj);
        }
    }
}
