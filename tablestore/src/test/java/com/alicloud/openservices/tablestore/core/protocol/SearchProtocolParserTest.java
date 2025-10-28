package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.protocol.Search.Aggregations;
import com.alicloud.openservices.tablestore.core.protocol.Search.GroupBySort;
import com.alicloud.openservices.tablestore.core.protocol.Search.GroupBys;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.search.agg.Aggregation;
import com.alicloud.openservices.tablestore.model.search.analysis.FuzzyAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SingleWordAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SplitAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.filter.SearchFilter;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupBy;
import com.alicloud.openservices.tablestore.model.search.groupby.GroupByGeoGrid;
import com.alicloud.openservices.tablestore.model.search.highlight.Highlight;
import com.alicloud.openservices.tablestore.model.search.query.InnerHits;
import com.alicloud.openservices.tablestore.model.search.query.Query;
import com.alicloud.openservices.tablestore.model.search.sort.GroupBySorter;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alicloud.openservices.tablestore.core.protocol.SearchGroupByParser.toGroupBy;
import static com.alicloud.openservices.tablestore.core.protocol.SearchInnerHitsBuilder.buildInnerHits;
import static com.alicloud.openservices.tablestore.core.protocol.SearchInnerHitsBuilder.buildInnerHitsToBytes;
import static com.alicloud.openservices.tablestore.core.protocol.SearchInnerHitsParser.toInnerHits;
import static org.junit.Assert.*;

public class SearchProtocolParserTest extends BaseSearchTest {

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
    public void toFieldSchema_FieldType() {
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

            Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
            builder.setFieldType(pbFieldType);

            FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
            assertEquals(modelFieldType, result.getFieldType());
        }
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
        boolean caseSensitive = random().nextBoolean();
        b.setCaseSensitive(caseSensitive);
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Split, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof SplitAnalyzerParameter);
        assertNull(((SplitAnalyzerParameter) result.getAnalyzerParameter()).getDelimiter());
        assertEquals(caseSensitive, ((SplitAnalyzerParameter) result.getAnalyzerParameter()).isCaseSensitive());
    }

    @Test
    public void toFieldSchema_Fuzzy() {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setAnalyzer("fuzzy");

        Search.FuzzyAnalyzerParameter.Builder b = Search.FuzzyAnalyzerParameter.newBuilder();
        b.setMinChars(2);
        b.setMaxChars(3);
        boolean caseSensitive = random().nextBoolean();
        b.setCaseSensitive(caseSensitive);
        builder.setAnalyzerParameter(b.build().toByteString());

        FieldSchema result = SearchProtocolParser.toFieldSchema(builder.build());
        assertEquals(FieldSchema.Analyzer.Fuzzy, result.getAnalyzer());
        assertTrue(result.getAnalyzerParameter() instanceof FuzzyAnalyzerParameter);
        assertEquals(2, ((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMinChars().intValue());
        assertEquals(3, ((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).getMaxChars().intValue());
        assertEquals(caseSensitive, ((FuzzyAnalyzerParameter) result.getAnalyzerParameter()).isCaseSensitive());
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
    public void testHighlightSerialization() throws IOException {
        {
            Highlight originHighlight = randomHighlight();
            Search.Highlight pbHighlight = SearchHighlightBuilder.buildHighlight(originHighlight);
            Highlight newHighlight = SearchHighlightParser.toHighlight(pbHighlight);
            assertJsonEquals(originHighlight, newHighlight);
        }
        {
            Highlight originHighlight = randomHighlight();
            byte[] pbHighlightBytes = SearchHighlightBuilder.buildHighlightToBytes(originHighlight);
            Highlight newHighlight = SearchHighlightParser.toHighlight(pbHighlightBytes);
            assertJsonEquals(originHighlight, newHighlight);
        }
    }

    @Test
    public void testInnerHitsSerialization() throws IOException {
        {
            InnerHits origiInnerHits = randomInnerHits();
            Search.InnerHits pbInnerHits = buildInnerHits(origiInnerHits);
            InnerHits newInnerHits = toInnerHits(pbInnerHits);
            assertJsonEquals(origiInnerHits, newInnerHits);
        }
        {
            InnerHits originInnerHits = randomInnerHits();
            byte[] pbInnerHitsBytes = buildInnerHitsToBytes(originInnerHits);
            InnerHits newInnerHits = toInnerHits(pbInnerHitsBytes);
            assertJsonEquals(originInnerHits, newInnerHits);
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

    @Test
    public void testToGeoHashPrecision() throws IOException {
        GeoHashPrecision[] precision = {GeoHashPrecision.GHP_5009KM_4992KM_1, GeoHashPrecision.GHP_1252KM_624KM_2, GeoHashPrecision.GHP_156KM_156KM_3,
                GeoHashPrecision.GHP_39KM_19KM_4, GeoHashPrecision.GHP_4900M_4900M_5, GeoHashPrecision.GHP_1200M_609M_6, GeoHashPrecision.GHP_152M_152M_7,
                GeoHashPrecision.GHP_38M_19M_8, GeoHashPrecision.GHP_480CM_480CM_9, GeoHashPrecision.GHP_120CM_595MM_10, GeoHashPrecision.GHP_149MM_149MM_11,
                GeoHashPrecision.GHP_37MM_19MM_12};

        Search.GeoHashPrecision[] pbPrecision = {Search.GeoHashPrecision.GHP_5009KM_4992KM_1, Search.GeoHashPrecision.GHP_1252KM_624KM_2, Search.GeoHashPrecision.GHP_156KM_156KM_3,
                Search.GeoHashPrecision.GHP_39KM_19KM_4, Search.GeoHashPrecision.GHP_4900M_4900M_5, Search.GeoHashPrecision.GHP_1200M_609M_6, Search.GeoHashPrecision.GHP_152M_152M_7,
                Search.GeoHashPrecision.GHP_38M_19M_8, Search.GeoHashPrecision.GHP_480CM_480CM_9, Search.GeoHashPrecision.GHP_120CM_595MM_10, Search.GeoHashPrecision.GHP_149MM_149MM_11,
                Search.GeoHashPrecision.GHP_37MM_19MM_12};
        for (int i = 0; i < 12; i++) {
            ByteString body =  Search.GroupByGeoGrid.newBuilder().setPrecision(pbPrecision[i]).build().toByteString();
            Search.GroupBy pbGroupBy =  Search.GroupBy.newBuilder().setName("group_by_geo_grid").setType(Search.GroupByType.GROUP_BY_GEO_GRID).setBody(body).build();
            GroupByGeoGrid groupBy = (GroupByGeoGrid)toGroupBy(pbGroupBy);
            assertEquals(precision[i], groupBy.getPrecision());
        }
    }

    @Test
    public void testTrackTotalCountInSearchQuerySerialization() throws IOException {
        // getTotalCount = true
        {
            SearchQuery origin = new SearchQuery();
            origin.setGetTotalCount(true);
            Search.SearchQuery pb = SearchProtocolBuilder.buildSearchQuery(origin);
            SearchQuery newObj = SearchProtocolParser.toSearchQuery(pb.toByteString());
            assertEquals(Integer.MAX_VALUE, newObj.getTrackTotalCount());
        }
        {
            SearchQuery origin = new SearchQuery();
            origin.setGetTotalCount(true);
            byte[] pb = SearchProtocolBuilder.buildSearchQueryToBytes(origin);
            SearchQuery newObj = SearchProtocolParser.toSearchQuery(pb);
            assertEquals(Integer.MAX_VALUE, newObj.getTrackTotalCount());
        }
        // getTotalCount = false
        {
            SearchQuery origin = new SearchQuery();
            origin.setGetTotalCount(false);
            Search.SearchQuery pb = SearchProtocolBuilder.buildSearchQuery(origin);
            SearchQuery newObj = SearchProtocolParser.toSearchQuery(pb.toByteString());
            assertEquals(-1, newObj.getTrackTotalCount());
        }
        {
            SearchQuery origin = new SearchQuery();
            origin.setGetTotalCount(false);
            byte[] pb = SearchProtocolBuilder.buildSearchQueryToBytes(origin);
            SearchQuery newObj = SearchProtocolParser.toSearchQuery(pb);
            assertEquals(-1, newObj.getTrackTotalCount());
        }
        // setTrackTotalCount = randInt()
        {
            SearchQuery origin = new SearchQuery();
            int randTrackTotalCount = random().nextInt();
            origin.setTrackTotalCount(randTrackTotalCount);
            Search.SearchQuery pb = SearchProtocolBuilder.buildSearchQuery(origin);
            SearchQuery newObj = SearchProtocolParser.toSearchQuery(pb.toByteString());
            assertEquals(randTrackTotalCount, newObj.getTrackTotalCount());
        }
        {
            SearchQuery origin = new SearchQuery();
            int randTrackTotalCount = random().nextInt();
            origin.setTrackTotalCount(randTrackTotalCount);
            byte[] pb = SearchProtocolBuilder.buildSearchQueryToBytes(origin);
            SearchQuery newObj = SearchProtocolParser.toSearchQuery(pb);
            assertEquals(randTrackTotalCount, newObj.getTrackTotalCount());
        }
    }

    @Test
    public void testFilterSerialization() throws IOException {
        {
            Query query = randomQuery();
            SearchFilter filter = SearchFilter.newBuilder().query(query).build();
            Search.SearchFilter pbFilter = SearchFilterBuilder.buildSearchFilter(filter);
            SearchFilter newFilter = SearchFilterParser.toSearchFilter(pbFilter);
            assertJsonEquals(filter, newFilter);
        }
    }

    @Test
    public void testFieldSchemaSerialization() throws IOException {
        // JSON_OBJECT
        {
            Search.FieldSchema pbFieldSchema = Search.FieldSchema.newBuilder()
                .setFieldName("field1")
                .setFieldType(Search.FieldType.JSON)
                .setJsonType(Search.JsonType.OBJECT_JSON)
                .addFieldSchemas(Search.FieldSchema.newBuilder().setFieldName("subField1").setFieldType(Search.FieldType.TEXT).build())
                .build();
            FieldSchema fieldSchema = SearchProtocolParser.toFieldSchema(pbFieldSchema);
            assertEquals(FieldType.JSON, fieldSchema.getFieldType());
            assertEquals(JsonType.OBJECT, fieldSchema.getJsonType());
            assertEquals(1, fieldSchema.getSubFieldSchemas().size());
            assertEquals(FieldType.TEXT, fieldSchema.getSubFieldSchemas().get(0).getFieldType());
            assertEquals("subField1", fieldSchema.getSubFieldSchemas().get(0).getFieldName());
        }
        // JSON_NESTED
        {
            Search.FieldSchema pbFieldSchema = Search.FieldSchema.newBuilder()
                .setFieldName("field1")
                .setFieldType(Search.FieldType.JSON)
                .setJsonType(Search.JsonType.NESTED_JSON)
                .addFieldSchemas(Search.FieldSchema.newBuilder().setFieldName("subField1").setFieldType(Search.FieldType.TEXT).build())
                .build();
            FieldSchema fieldSchema = SearchProtocolParser.toFieldSchema(pbFieldSchema);
            assertEquals(FieldType.JSON, fieldSchema.getFieldType());
            assertEquals(JsonType.NESTED, fieldSchema.getJsonType());
            assertEquals(1, fieldSchema.getSubFieldSchemas().size());
            assertEquals(FieldType.TEXT, fieldSchema.getSubFieldSchemas().get(0).getFieldType());
            assertEquals("subField1", fieldSchema.getSubFieldSchemas().get(0).getFieldName());
        }
        {
            Search.FieldSchema pbFieldSchema = Search.FieldSchema.newBuilder()
                    .setFieldName("field1")
                    .setFieldType(Search.FieldType.FLATTENED)
                    .setSortAndAgg(false)
                    .setStore(false)
                    .setIndex(false)
                    .build();
            FieldSchema fieldSchema = SearchProtocolParser.toFieldSchema(pbFieldSchema);
            assertEquals(FieldType.FLATTENED, fieldSchema.getFieldType());
            assertFalse(fieldSchema.isEnableSortAndAgg());
            assertFalse(fieldSchema.isStore());
            assertFalse(fieldSchema.isIndex());
        }
    }
}
