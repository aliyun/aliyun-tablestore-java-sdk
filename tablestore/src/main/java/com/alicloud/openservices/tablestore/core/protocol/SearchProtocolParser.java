package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.ReservedThroughput;
import com.alicloud.openservices.tablestore.model.search.Collapse;
import com.alicloud.openservices.tablestore.model.search.DateTimeUnit;
import com.alicloud.openservices.tablestore.model.search.DateTimeValue;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexOptions;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.model.search.IndexSetting;
import com.alicloud.openservices.tablestore.model.search.JsonType;
import com.alicloud.openservices.tablestore.model.search.MeteringInfo;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.QueryFlowWeight;
import com.alicloud.openservices.tablestore.model.search.ScanQuery;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchRequest.ColumnsToGet;
import com.alicloud.openservices.tablestore.model.search.SyncStat;
import com.alicloud.openservices.tablestore.model.search.analysis.FuzzyAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SingleWordAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.analysis.SplitAnalyzerParameter;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.PrimaryKeySort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.alicloud.openservices.tablestore.model.search.vector.VectorDataType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorMetricType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorOptions;
import com.aliyun.ots.thirdparty.com.google.protobuf.ByteString;
import com.aliyun.ots.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SearchProtocolParser {

    private static FieldType toFieldType(Search.FieldType fieldType) {
        switch (fieldType) {
            case LONG:
                return FieldType.LONG;
            case DOUBLE:
                return FieldType.DOUBLE;
            case BOOLEAN:
                return FieldType.BOOLEAN;
            case KEYWORD:
                return FieldType.KEYWORD;
            case TEXT:
                return FieldType.TEXT;
            case NESTED:
                return FieldType.NESTED;
            case GEO_POINT:
                return FieldType.GEO_POINT;
            case DATE:
                return FieldType.DATE;
            case VECTOR:
                return FieldType.VECTOR;
            case FUZZY_KEYWORD:
                return FieldType.FUZZY_KEYWORD;
            case IP:
                return FieldType.IP;
            case JSON:
                return FieldType.JSON;
            case FLATTENED:
                return FieldType.FLATTENED;
            default:
                return FieldType.UNKNOWN;
        }
    }

    private static IndexOptions toIndexOptions(Search.IndexOptions indexOptions) {
        switch (indexOptions) {
            case DOCS:
                return IndexOptions.DOCS;
            case FREQS:
                return IndexOptions.FREQS;
            case POSITIONS:
                return IndexOptions.POSITIONS;
            case OFFSETS:
                return IndexOptions.OFFSETS;
            default:
                throw new IllegalArgumentException("Unknown indexOptions: " + indexOptions.name());
        }
    }

    private static SingleWordAnalyzerParameter toAnalyzerParameter(Search.SingleWordAnalyzerParameter analyzerParameter) {
        SingleWordAnalyzerParameter result = new SingleWordAnalyzerParameter();
        if (analyzerParameter.hasCaseSensitive()) {
            result.setCaseSensitive(analyzerParameter.getCaseSensitive());
        }
        if (analyzerParameter.hasDelimitWord()) {
            result.setDelimitWord(analyzerParameter.getDelimitWord());
        }
        return result;
    }

    private static SplitAnalyzerParameter toAnalyzerParameter(Search.SplitAnalyzerParameter analyzerParameter) {
        SplitAnalyzerParameter result = new SplitAnalyzerParameter();
        if (analyzerParameter.hasDelimiter()) {
            result.setDelimiter(analyzerParameter.getDelimiter());
        }
        if (analyzerParameter.hasCaseSensitive()) {
            result.setCaseSensitive(analyzerParameter.getCaseSensitive());
        }
        return result;
    }

    private static FuzzyAnalyzerParameter toAnalyzerParameter(Search.FuzzyAnalyzerParameter analyzerParameter) {
        FuzzyAnalyzerParameter result = new FuzzyAnalyzerParameter();
        if (analyzerParameter.hasMinChars()) {
            result.setMinChars(analyzerParameter.getMinChars());
        }
        if (analyzerParameter.hasMaxChars()) {
            result.setMaxChars(analyzerParameter.getMaxChars());
        }
        if (analyzerParameter.hasCaseSensitive()) {
            result.setCaseSensitive(analyzerParameter.getCaseSensitive());
        }
        return result;
    }

    static FieldSchema toFieldSchema(Search.FieldSchema fieldSchema) {
        FieldSchema result = new FieldSchema(fieldSchema.getFieldName(),
                toFieldType(fieldSchema.getFieldType()));
        if (fieldSchema.hasIndex()) {
            result.setIndex(fieldSchema.getIndex());
        }
        if (fieldSchema.hasSortAndAgg()) {
            result.setEnableSortAndAgg(fieldSchema.getSortAndAgg());
        }
        if (fieldSchema.hasEnableHighlighting()) {
            result.setEnableHighlighting(fieldSchema.getEnableHighlighting());
        }
        if (fieldSchema.hasStore()) {
            result.setStore(fieldSchema.getStore());
        }
        if (fieldSchema.hasIsArray()) {
            result.setIsArray(fieldSchema.getIsArray());
        }
        if (fieldSchema.hasIndexOptions()) {
            result.setIndexOptions(toIndexOptions(fieldSchema.getIndexOptions()));
        }
        if (fieldSchema.hasAnalyzer()) {
            result.setAnalyzer(FieldSchema.Analyzer.fromString(fieldSchema.getAnalyzer()));
        }
        if (fieldSchema.hasAnalyzerParameter()) {
            FieldSchema.Analyzer analyzer = FieldSchema.Analyzer.fromString(fieldSchema.getAnalyzer());
            try {
                switch (analyzer) {
                    case SingleWord:
                        result.setAnalyzerParameter(toAnalyzerParameter(
                                Search.SingleWordAnalyzerParameter.parseFrom(fieldSchema.getAnalyzerParameter())
                        ));
                        break;
                    case Split:
                        result.setAnalyzerParameter(toAnalyzerParameter(
                                Search.SplitAnalyzerParameter.parseFrom(fieldSchema.getAnalyzerParameter())
                        ));
                        break;
                    case Fuzzy:
                        result.setAnalyzerParameter(toAnalyzerParameter(
                                Search.FuzzyAnalyzerParameter.parseFrom(fieldSchema.getAnalyzerParameter())
                        ));
                        break;
                }
            } catch (InvalidProtocolBufferException e) {
                throw new ClientException("failed to parse single_word analyzer parameter: " + e.getMessage());
            }
        }

        if (fieldSchema.getFieldSchemasList() != null) {
            List<FieldSchema> subSchemas = new ArrayList<FieldSchema>();
            for (Search.FieldSchema subSchema : fieldSchema.getFieldSchemasList()) {
                subSchemas.add(toFieldSchema(subSchema));
            }
            result.setSubFieldSchemas(subSchemas);
        }
        if (fieldSchema.hasIsVirtualField()) {
            result.setVirtualField(fieldSchema.getIsVirtualField());
        }
        result.setSourceFieldNames(fieldSchema.getSourceFieldNamesList());
        result.setDateFormats(fieldSchema.getDateFormatsList());
        if (fieldSchema.hasVectorOptions()) {
            result.setVectorOptions(toVectorOptions(fieldSchema.getVectorOptions()));
        }
        if (fieldSchema.hasJsonType()) {
            result.setJsonType(toJsonType(fieldSchema.getJsonType()));
        }
        return result;
    }

    public static VectorOptions toVectorOptions(Search.VectorOptions pbOptions) {
        VectorOptions vectorOptions = new VectorOptions();
        if (pbOptions.hasDataType()) {
            vectorOptions.setDataType(toVectorMetricType(pbOptions.getDataType()));
        }
        if (pbOptions.hasDimension()) {
            vectorOptions.setDimension(pbOptions.getDimension());
        }
        if (pbOptions.hasMetricType()) {
            vectorOptions.setMetricType(toVectorMetricType(pbOptions.getMetricType()));
        }
        return vectorOptions;
    }

    private static VectorDataType toVectorMetricType(Search.VectorDataType type) {
        switch (type) {
            case VD_FLOAT_32:
                return VectorDataType.FLOAT_32;
            default:
                throw new IllegalArgumentException("unknown vector data type type:" + type.name());
        }
    }

    private static VectorMetricType toVectorMetricType(Search.VectorMetricType type) {
        switch (type) {
            case VM_EUCLIDEAN:
                return VectorMetricType.EUCLIDEAN;
            case VM_COSINE:
                return VectorMetricType.COSINE;
            case VM_DOT_PRODUCT:
                return VectorMetricType.DOT_PRODUCT;
            default:
                throw new IllegalArgumentException("unknown vector metric type type:" + type.name());
        }
    }

    private static JsonType toJsonType(Search.JsonType jsonType) {
        switch (jsonType) {
            case OBJECT_JSON:
                return JsonType.OBJECT;
            case NESTED_JSON:
                return JsonType.NESTED;
            default:
                throw new IllegalArgumentException("unknown json type: " + jsonType.name());
        }
    }

    private static IndexSetting toIndexSetting(Search.IndexSetting indexSetting) {
        IndexSetting result = new IndexSetting();
        if (indexSetting.getRoutingFieldsCount() > 0) {
            result.setRoutingFields(indexSetting.getRoutingFieldsList());
        }
        if (indexSetting.hasEnableCustomColumnVersion()) {
            result.setEnableCustomColumnVersion(indexSetting.getEnableCustomColumnVersion());
        }
        return result;
    }

    private static Sort toIndexSort(Search.Sort sort) {
        if (sort.getSorterCount() == 0) {
            return null;
        }
        List<Sort.Sorter> sorters = new ArrayList<Sort.Sorter>();
        for (Search.Sorter sorter : sort.getSorterList()) {
            if (sorter.hasFieldSort()) {
                Search.FieldSort pbFieldSort = sorter.getFieldSort();
                FieldSort fieldSort = new FieldSort(pbFieldSort.getFieldName());
                if (pbFieldSort.hasOrder()) {
                    if (pbFieldSort.getOrder().equals(Search.SortOrder.SORT_ORDER_ASC)) {
                        fieldSort.setOrder(SortOrder.ASC);
                    } else {
                        fieldSort.setOrder(SortOrder.DESC);
                    }
                }
                sorters.add(fieldSort);
            } else if (sorter.hasPkSort()) {
                Search.PrimaryKeySort pbPkSort = sorter.getPkSort();
                PrimaryKeySort pkSort = new PrimaryKeySort();
                if (pbPkSort.hasOrder()) {
                    if (pbPkSort.getOrder().equals(Search.SortOrder.SORT_ORDER_ASC)) {
                        pkSort.setOrder(SortOrder.ASC);
                    } else {
                        pkSort.setOrder(SortOrder.DESC);
                    }
                }
                sorters.add(pkSort);
            } else {
                throw new ClientException("failed to parse index_sort in response");
            }
        }
        return new Sort(sorters);
    }

    static IndexSchema toIndexSchema(Search.IndexSchema indexSchema) {
        IndexSchema result = new IndexSchema();
        result.setIndexSetting(toIndexSetting(indexSchema.getIndexSetting()));
        List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        for (Search.FieldSchema fieldSchema : indexSchema.getFieldSchemasList()) {
            fieldSchemas.add(toFieldSchema(fieldSchema));
        }
        result.setFieldSchemas(fieldSchemas);
        if (indexSchema.hasIndexSort()) {
            result.setIndexSort(toIndexSort(indexSchema.getIndexSort()));
        }
        return result;
    }

    static SyncStat toSyncStat(Search.SyncStat syncStat) {
        SyncStat result = new SyncStat();
        if (!syncStat.hasSyncPhase()) {
            throw new ClientException("missing [SyncPhase] in SyncStat");
        }
        switch (syncStat.getSyncPhase()) {
            case FULL:
                result.setSyncPhase(SyncStat.SyncPhase.FULL);
                break;
            case INCR:
                result.setSyncPhase(SyncStat.SyncPhase.INCR);
                break;
            default:
                throw new ClientException("unknown SyncPhase: " + syncStat.getSyncPhase().name());
        }
        if (syncStat.hasCurrentSyncTimestamp()) {
            result.setCurrentSyncTimestamp(syncStat.getCurrentSyncTimestamp());
        }
        return result;
    }

    static MeteringInfo toMeteringInfo(Search.MeteringInfo meteringInfo) {
        MeteringInfo result = new MeteringInfo();
        if (meteringInfo.hasReservedReadCu()) {
            result.setReservedThroughput(new ReservedThroughput(
                    new CapacityUnit((int) meteringInfo.getReservedReadCu(), 0)));
        }
        if (meteringInfo.hasStorageSize()) {
            result.setStorageSize(meteringInfo.getStorageSize());
        }
        if (meteringInfo.hasRowCount()) {
            result.setRowCount(meteringInfo.getRowCount());
        }
        if (meteringInfo.hasTimestamp()) {
            result.setTimestamp(meteringInfo.getTimestamp());
        }
        return result;
    }

    static DescribeSearchIndexResponse.IndexStatus toIndexStatus(Search.IndexStatus pbIndexStatus) {
        DescribeSearchIndexResponse.IndexStatus indexStatus = new DescribeSearchIndexResponse.IndexStatus();

        if (pbIndexStatus.hasStatus()) {
            switch (pbIndexStatus.getStatus()) {
                case PENDING:
                    indexStatus.indexStatusEnum = DescribeSearchIndexResponse.IndexStatusEnum.PENDING;
                    break;
                case FAILED:
                    indexStatus.indexStatusEnum = DescribeSearchIndexResponse.IndexStatusEnum.FAILED;
                    break;
                case RUNNING:
                    indexStatus.indexStatusEnum = DescribeSearchIndexResponse.IndexStatusEnum.RUNNING;
                    break;
                default:
                    indexStatus.indexStatusEnum = DescribeSearchIndexResponse.IndexStatusEnum.UNKNOWN;
                    break;
            }
        } else {
            indexStatus.indexStatusEnum = DescribeSearchIndexResponse.IndexStatusEnum.UNKNOWN;
        }

        if (pbIndexStatus.hasStatusDescription()) {
            indexStatus.statusDescription = pbIndexStatus.getStatusDescription();
        }

        return indexStatus;
    }

    static QueryFlowWeight toQueryFlowWeight(Search.QueryFlowWeight queryFlowWeight) {
        if (!queryFlowWeight.hasIndexName()) {
            throw new ClientException("[query_flow_weight] has no index name");
        }
        if (!queryFlowWeight.hasWeight()) {
            throw new ClientException("[query_flow_weight] has no weight");
        }
        return new QueryFlowWeight(queryFlowWeight.getIndexName(), queryFlowWeight.getWeight());
    }

    public static SearchQuery toSearchQuery(ByteString byteString) throws IOException {
        return toSearchQuery(byteString.toByteArray());
    }

    public static SearchQuery toSearchQuery(byte[] bytes) throws IOException {
        SearchQuery searchQuery = new SearchQuery();
        Search.SearchQuery pb = Search.SearchQuery.parseFrom(bytes);
        if (pb.hasOffset()) {
            searchQuery.setOffset(pb.getOffset());
        }
        if (pb.hasLimit()) {
            searchQuery.setLimit(pb.getLimit());
        }
        if (pb.hasQuery()) {
            searchQuery.setQuery(SearchQueryParser.toQuery(pb.getQuery()));
        }
        if (pb.hasHighlight()) {
            searchQuery.setHighlight(SearchHighlightParser.toHighlight(pb.getHighlight()));
        }
        if (pb.hasSort()) {
            searchQuery.setSort(SearchSortParser.toSort(pb.getSort()));
        }
        if (pb.hasCollapse()) {
            Search.Collapse collapse = pb.getCollapse();
            String fieldName = collapse.getFieldName();
            searchQuery.setCollapse(new Collapse(fieldName));
        }
        if (pb.hasTrackTotalCount()) {
            searchQuery.setTrackTotalCount(pb.getTrackTotalCount());
        }
        if (pb.hasToken()) {
            searchQuery.setToken(pb.getToken().toByteArray());
        }
        if (pb.hasAggs()) {
            searchQuery.setAggregationList(SearchAggregationParser.toAggregations(pb.getAggs()));
        }
        if (pb.hasGroupBys()) {
            searchQuery.setGroupByList(SearchGroupByParser.toGroupBys(pb.getGroupBys()));
        }
        if (pb.hasFilter()){
            searchQuery.setFilter(SearchFilterParser.toSearchFilter(pb.getFilter()));
        }
        return searchQuery;
    }

    public static ColumnsToGet toColumnsToGet(Search.ColumnsToGet pb) {
        ColumnsToGet columnsToGet = new ColumnsToGet();
        columnsToGet.setColumns(pb.getColumnNamesList());
        Preconditions.checkArgument(pb.hasReturnType(), "Search.ColumnsToGet must has 'ReturnType'");
        switch (pb.getReturnType()) {
            case RETURN_ALL:
                columnsToGet.setReturnAll(true);
                break;
            case RETURN_ALL_FROM_INDEX:
                columnsToGet.setReturnAllFromIndex(true);
                break;
            case RETURN_SPECIFIED:
            case RETURN_NONE:
            default:
                break;
        }
        return columnsToGet;
    }

    public static List<PrimaryKey> toRoutingValues(List<ByteString> byteStringList) throws IOException {
        List<PrimaryKey> primaryKeys = new ArrayList<PrimaryKey>();
        for (ByteString byteString : byteStringList) {
            PlainBufferCodedInputStream coded = new PlainBufferCodedInputStream(new PlainBufferInputStream(byteString.asReadOnlyByteBuffer()));
            Preconditions.checkArgument(coded.readHeader() == PlainBufferConsts.HEADER, "check plain buff header failed");
            coded.readTag();
            List<PlainBufferCell> plainBufferCells = coded.readRowPK();
            PrimaryKey primaryKey = PlainBufferConversion.toPrimaryKey(plainBufferCells);
            primaryKeys.add(primaryKey);
        }
        return primaryKeys;
    }

    public static SearchRequest toSearchRequest(ByteString byteString) throws IOException {
        return toSearchRequest(byteString.toByteArray());
    }

    public static SearchRequest toSearchRequest(byte[] bytes) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        Search.SearchRequest pb = Search.SearchRequest.parseFrom(bytes);
        if (pb.hasTableName()) {
            searchRequest.setTableName(pb.getTableName());
        }
        if (pb.hasIndexName()) {
            searchRequest.setIndexName(pb.getIndexName());
        }
        if (pb.hasColumnsToGet()) {
            searchRequest.setColumnsToGet(toColumnsToGet(pb.getColumnsToGet()));
        }
        if (pb.hasSearchQuery()) {
            searchRequest.setSearchQuery(toSearchQuery(pb.getSearchQuery()));
        }
        if (!pb.getRoutingValuesList().isEmpty()) {
            searchRequest.setRoutingValues(toRoutingValues(pb.getRoutingValuesList()));
        }
        if (pb.hasTimeoutMs() && pb.getTimeoutMs() > 0) {
            searchRequest.setTimeoutInMillisecond(pb.getTimeoutMs());
        }
        return searchRequest;
    }

    public static ScanQuery toScanQuery(ByteString byteString) throws IOException {
        return toScanQuery(byteString.toByteArray());
    }

    public static ScanQuery toScanQuery(byte[] bytes) throws IOException {
        ScanQuery scanQuery = new ScanQuery();
        Search.ScanQuery pb = Search.ScanQuery.parseFrom(bytes);
        if (pb.hasLimit()) {
            scanQuery.setLimit(pb.getLimit());
        }
        if (pb.hasQuery()) {
            scanQuery.setQuery(SearchQueryParser.toQuery(pb.getQuery()));
        }
        if (pb.hasToken()) {
            scanQuery.setToken(pb.getToken().toByteArray());
        }
        if (pb.hasMaxParallel()) {
            scanQuery.setMaxParallel(pb.getMaxParallel());
        }
        if (pb.hasCurrentParallelId()) {
            scanQuery.setCurrentParallelId(pb.getCurrentParallelId());
        }
        if (pb.hasAliveTime()) {
            scanQuery.setAliveTime(pb.getAliveTime());
        }
        return scanQuery;
    }

    public static ParallelScanRequest toParallelScanRequest(ByteString byteString) throws IOException {
        return toParallelScanRequest(byteString.toByteArray());
    }

    public static ParallelScanRequest toParallelScanRequest(byte[] bytes) throws IOException {
        ParallelScanRequest request = new ParallelScanRequest();
        Search.ParallelScanRequest pb = Search.ParallelScanRequest.parseFrom(bytes);
        if (pb.hasTableName()) {
            request.setTableName(pb.getTableName());
        }
        if (pb.hasIndexName()) {
            request.setIndexName(pb.getIndexName());
        }
        if (pb.hasColumnsToGet()) {
            request.setColumnsToGet(toColumnsToGet(pb.getColumnsToGet()));
        }
        if (pb.hasSessionId()) {
            request.setSessionId(pb.getSessionId().toByteArray());
        }
        if (pb.hasScanQuery()) {
            request.setScanQuery(toScanQuery(pb.getScanQuery().toByteString()));
        }
        if (pb.hasTimeoutMs() && pb.getTimeoutMs() > 0) {
            request.setTimeoutInMillisecond(pb.getTimeoutMs());
        }
        return request;
    }

    public static DateTimeUnit toDateTimeUnit(Search.DateTimeUnit unit) {
        switch (unit) {
            case YEAR:
                return DateTimeUnit.YEAR;
            case QUARTER_YEAR:
                return DateTimeUnit.QUARTER_YEAR;
            case MONTH:
                return DateTimeUnit.MONTH;
            case WEEK:
                return DateTimeUnit.WEEK;
            case DAY:
                return DateTimeUnit.DAY;
            case HOUR:
                return DateTimeUnit.HOUR;
            case MINUTE:
                return DateTimeUnit.MINUTE;
            case SECOND:
                return DateTimeUnit.SECOND;
            case MILLISECOND:
                return DateTimeUnit.MILLISECOND;
            default:
                throw new IllegalArgumentException("Unknown DateTimeUnit: " + unit.name());
        }
    }

    public static DateTimeValue toDateTimeValue(Search.DateTimeValue pb) {
        DateTimeValue dateTimeValue = new DateTimeValue();
        if (pb.hasValue()) {
            dateTimeValue.setValue(pb.getValue());
        }
        if (pb.hasUnit()) {
            DateTimeUnit dateTimeUnit = toDateTimeUnit(pb.getUnit());
            dateTimeValue.setUnit(dateTimeUnit);
        }
        return dateTimeValue;
    }
}
