package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.core.protocol.Search.ColumnReturnType;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.search.Collapse;
import com.alicloud.openservices.tablestore.model.search.CreateSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DateTimeUnit;
import com.alicloud.openservices.tablestore.model.search.DateTimeValue;
import com.alicloud.openservices.tablestore.model.search.DeleteSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexOptions;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.model.search.IndexSetting;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.QueryFlowWeight;
import com.alicloud.openservices.tablestore.model.search.ScanQuery;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchRequest.ColumnsToGet;
import com.alicloud.openservices.tablestore.model.search.UpdateSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.vector.VectorDataType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorMetricType;
import com.alicloud.openservices.tablestore.model.search.vector.VectorOptions;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SearchProtocolBuilder {

    private static final int DEFAULT_NUMBER_OF_SHARDS = 1;

    private static Search.FieldType buildFieldType(FieldType fieldType) {
        switch (fieldType) {
            case LONG:
                return Search.FieldType.LONG;
            case DOUBLE:
                return Search.FieldType.DOUBLE;
            case BOOLEAN:
                return Search.FieldType.BOOLEAN;
            case KEYWORD:
                return Search.FieldType.KEYWORD;
            case TEXT:
                return Search.FieldType.TEXT;
            case NESTED:
                return Search.FieldType.NESTED;
            case GEO_POINT:
                return Search.FieldType.GEO_POINT;
            case DATE:
                return Search.FieldType.DATE;
            case VECTOR:
                return Search.FieldType.VECTOR;
            case FUZZY_KEYWORD:
                return Search.FieldType.FUZZY_KEYWORD;
            default:
                throw new IllegalArgumentException("Unknown fieldType: " + fieldType.name());
        }
    }

    private static Search.IndexOptions buildIndexOptions(IndexOptions indexOptions) {
        switch (indexOptions) {
            case DOCS:
                return Search.IndexOptions.DOCS;
            case FREQS:
                return Search.IndexOptions.FREQS;
            case POSITIONS:
                return Search.IndexOptions.POSITIONS;
            case OFFSETS:
                return Search.IndexOptions.OFFSETS;
            default:
                throw new IllegalArgumentException("Unknown indexOptions: " + indexOptions.name());
        }
    }

    static Search.FieldSchema buildFieldSchema(FieldSchema fieldSchema) {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setFieldName(fieldSchema.getFieldName());
        builder.setFieldType(buildFieldType(fieldSchema.getFieldType()));
        if (fieldSchema.getFieldType() != FieldType.NESTED) {
            if (fieldSchema.isIndex() != null) {
                builder.setIndex(fieldSchema.isIndex());
            }
            if (fieldSchema.isEnableSortAndAgg() != null) {
                builder.setSortAndAgg(fieldSchema.isEnableSortAndAgg());
            }
            if (fieldSchema.isStore() != null) {
                builder.setStore(fieldSchema.isStore());
            } else {
                if (fieldSchema.getFieldType() == FieldType.TEXT) {
                    builder.setStore(false);
                } else {
                    builder.setStore(true);
                }
            }
            if (fieldSchema.isArray() != null) {
                builder.setIsArray(fieldSchema.isArray());
            }
            if (fieldSchema.isEnableHighlighting() != null) {
                builder.setEnableHighlighting(fieldSchema.isEnableHighlighting());
            }
        }
        if (fieldSchema.getIndexOptions() != null) {
            builder.setIndexOptions(buildIndexOptions(fieldSchema.getIndexOptions()));
        }
        if (fieldSchema.getAnalyzer() != null) {
            builder.setAnalyzer(fieldSchema.getAnalyzer().toString());

            if (fieldSchema.getAnalyzerParameter() != null) {
                switch (fieldSchema.getAnalyzer()) {
                    case SingleWord:
                    case Split:
                    case Fuzzy:
                        builder.setAnalyzerParameter(fieldSchema.getAnalyzerParameter().serialize());
                        break;
                    default:
                        break;
                }
            }
        }
        if (fieldSchema.getSubFieldSchemas() != null) {
            for (FieldSchema subSchema : fieldSchema.getSubFieldSchemas()) {
                builder.addFieldSchemas(buildFieldSchema(subSchema));
            }
        }
        if (fieldSchema.isVirtualField() != null) {
            builder.setIsVirtualField(fieldSchema.isVirtualField());
        }
        if (fieldSchema.getSourceFieldNames() != null) {
            for (String sourceField : fieldSchema.getSourceFieldNames()) {
                builder.addSourceFieldNames(sourceField);
            }
        }
        if (fieldSchema.getDateFormats() != null) {
            for (String dateFormat : fieldSchema.getDateFormats()) {
                builder.addDateFormats(dateFormat);
            }
        }
        if (fieldSchema.getVectorOptions() != null) {
            builder.setVectorOptions(buildVectorOptions(fieldSchema.getVectorOptions()));
        }
        return builder.build();
    }

    public static Search.VectorOptions buildVectorOptions(VectorOptions vectorOptions) {
        Search.VectorOptions.Builder builder = Search.VectorOptions.newBuilder();
        if (vectorOptions.getDataType() != null) {
            builder.setDataType(buildVectorMetricType(vectorOptions.getDataType()));
        }
        if (vectorOptions.getDimension() != null) {
            builder.setDimension(vectorOptions.getDimension());
        }
        if (vectorOptions.getMetricType() != null) {
            builder.setMetricType(buildVectorMetricType(vectorOptions.getMetricType()));
        }
        return builder.build();
    }

    private static Search.VectorDataType buildVectorMetricType(VectorDataType type) {
        switch (type) {
            case FLOAT_32:
                return Search.VectorDataType.VD_FLOAT_32;
            default:
                throw new IllegalArgumentException("unknown vector data type type:" + type.name());
        }
    }

    private static Search.VectorMetricType buildVectorMetricType(VectorMetricType type) {
        switch (type) {
            case EUCLIDEAN:
                return Search.VectorMetricType.VM_EUCLIDEAN;
            case COSINE:
                return Search.VectorMetricType.VM_COSINE;
            case DOT_PRODUCT:
                return Search.VectorMetricType.VM_DOT_PRODUCT;
            default:
                throw new IllegalArgumentException("unknown vector metric type type:" + type.name());
        }
    }

    private static Search.IndexSetting buildIndexSetting(IndexSetting indexSetting) {
        Search.IndexSetting.Builder builder = Search.IndexSetting.newBuilder();
        builder.setNumberOfShards(DEFAULT_NUMBER_OF_SHARDS);
        if (indexSetting.getRoutingFields() != null) {
            builder.addAllRoutingFields(indexSetting.getRoutingFields());
        }
        return builder.build();
    }

    private static Search.IndexSchema buildIndexSchema(IndexSchema indexSchema) {
        Search.IndexSchema.Builder builder = Search.IndexSchema.newBuilder();
        if (indexSchema.getIndexSetting() != null) {
            builder.setIndexSetting(buildIndexSetting(indexSchema.getIndexSetting()));
        } else {
            builder.setIndexSetting(buildIndexSetting(new IndexSetting()));
        }
        for (FieldSchema fieldSchema : indexSchema.getFieldSchemas()) {
            builder.addFieldSchemas(buildFieldSchema(fieldSchema));
        }
        if (indexSchema.getIndexSort() != null) {
            builder.setIndexSort(SearchSortBuilder.buildSort(indexSchema.getIndexSort()));
        }
        return builder.build();
    }

    public static Search.CreateSearchIndexRequest buildCreateSearchIndexRequest(CreateSearchIndexRequest request) {
        Search.CreateSearchIndexRequest.Builder builder = Search.CreateSearchIndexRequest.newBuilder();
        builder.setTableName(request.getTableName());
        builder.setIndexName(request.getIndexName());
        builder.setSchema(buildIndexSchema(request.getIndexSchema()));
        if (request.getSourceIndexName() != null) {
            builder.setSourceIndexName(request.getSourceIndexName());
        }
        if (request.getTimeToLive() != null) {
            builder.setTimeToLive(request.getTimeToLive());
        }
        return builder.build();
    }

    static Search.QueryFlowWeight buildQueryFlowWeight(QueryFlowWeight queryFlowWeight) {
        Search.QueryFlowWeight.Builder builder = Search.QueryFlowWeight.newBuilder();
        if (queryFlowWeight.getIndexName() == null) {
            throw new ClientException("[query_flow_weight.index_name] must not be null");
        }
        if (queryFlowWeight.getWeight() == null) {
            throw new ClientException("[query_flow_weight.weight] must not be null");
        }

        builder.setIndexName(queryFlowWeight.getIndexName());
        builder.setWeight(queryFlowWeight.getWeight());
        return builder.build();
    }

    public static Search.UpdateSearchIndexRequest buildUpdateSearchIndexRequest(UpdateSearchIndexRequest request) {
        Search.UpdateSearchIndexRequest.Builder builder = Search.UpdateSearchIndexRequest.newBuilder();
        builder.setTableName(request.getTableName());
        builder.setIndexName(request.getIndexName());

        if (request.getSwitchIndexName() != null) {
            builder.setSwitchIndexName(request.getSwitchIndexName());
        } else if (request.getQueryFlowWeight() != null && request.getQueryFlowWeight().size() > 0) {
            List<QueryFlowWeight> queryFlowWeight = request.getQueryFlowWeight();
            if (queryFlowWeight.size() != 2) {
                throw new ClientException("[query_flow_weight] size must be 2");
            }
            for (QueryFlowWeight fw : queryFlowWeight) {
                builder.addQueryFlowWeight(buildQueryFlowWeight(fw));
            }
        }
        if (request.getTimeToLive() != null) {
            builder.setTimeToLive(request.getTimeToLive());
        }
        return builder.build();
    }

    public static Search.DeleteSearchIndexRequest buildDeleteSearchIndexRequest(DeleteSearchIndexRequest request) {
        Search.DeleteSearchIndexRequest.Builder builder = Search.DeleteSearchIndexRequest.newBuilder();
        builder.setTableName(request.getTableName());
        builder.setIndexName(request.getIndexName());
        return builder.build();
    }

    public static Search.ListSearchIndexRequest buildListSearchIndexRequest(ListSearchIndexRequest request) {
        Search.ListSearchIndexRequest.Builder builder = Search.ListSearchIndexRequest.newBuilder();
        builder.setTableName(request.getTableName());
        return builder.build();
    }

    public static Search.DescribeSearchIndexRequest buildDescribeSearchIndexRequest(
            DescribeSearchIndexRequest request) {
        Search.DescribeSearchIndexRequest.Builder builder = Search.DescribeSearchIndexRequest.newBuilder();
        builder.setTableName(request.getTableName());
        builder.setIndexName(request.getIndexName());
        builder.setIncludeSyncStat(request.isIncludeSyncStat());
        return builder.build();
    }

    private static Search.ColumnsToGet buildColumnsToGet(ColumnsToGet columnsToGet) {
        Search.ColumnsToGet.Builder builder = Search.ColumnsToGet.newBuilder();
        if (columnsToGet.isReturnAll()) {
            builder.setReturnType(Search.ColumnReturnType.RETURN_ALL);
        } else if (columnsToGet.isReturnAllFromIndex()) {
            builder.setReturnType(ColumnReturnType.RETURN_ALL_FROM_INDEX);
        } else if (columnsToGet.getColumns() != null && columnsToGet.getColumns().size() > 0) {
            builder.setReturnType(Search.ColumnReturnType.RETURN_SPECIFIED);
            builder.addAllColumnNames(columnsToGet.getColumns());
        } else {
            builder.setReturnType(Search.ColumnReturnType.RETURN_NONE);
        }
        return builder.build();
    }

    private static Search.Collapse buildCollapse(Collapse collapse) {
        Search.Collapse.Builder builder = Search.Collapse.newBuilder();
        builder.setFieldName(collapse.getFieldName());
        return builder.build();
    }

    public static byte[] buildSearchQueryToBytes(SearchQuery searchQuery) {
        return buildSearchQuery(searchQuery).toByteArray();
    }

    static Search.SearchQuery buildSearchQuery(SearchQuery searchQuery) {
        Search.SearchQuery.Builder builder = Search.SearchQuery.newBuilder();
        if (searchQuery.getOffset() != null) {
            builder.setOffset(searchQuery.getOffset());
        }
        if (searchQuery.getLimit() != null) {
            builder.setLimit(searchQuery.getLimit());
        }
        if (searchQuery.getQuery() != null) {
            builder.setQuery(SearchQueryBuilder.buildQuery(searchQuery.getQuery()));
        }
        if (searchQuery.getHighlight() != null) {
            builder.setHighlight(SearchHighlightBuilder.buildHighlight(searchQuery.getHighlight()));
        }
        if (searchQuery.getSort() != null) {
            builder.setSort(SearchSortBuilder.buildSort(searchQuery.getSort()));
        }
        if (searchQuery.getCollapse() != null) {
            builder.setCollapse(buildCollapse(searchQuery.getCollapse()));
        }
        builder.setTrackTotalCount(searchQuery.getTrackTotalCount());
        if (searchQuery.getToken() != null) {
            builder.setToken(ByteString.copyFrom(searchQuery.getToken()));
        }
        if (searchQuery.getAggregationList() != null) {
            builder.setAggs(SearchAggregationBuilder.buildAggregations(searchQuery.getAggregationList()));
        }
        if (searchQuery.getGroupByList() != null) {
            builder.setGroupBys(SearchGroupByBuilder.buildGroupBys(searchQuery.getGroupByList()));
        }
        if (searchQuery.getFilter() != null) {
            builder.setFilter(SearchFilterBuilder.buildSearchFilter(searchQuery.getFilter()));
        }
        return builder.build();
    }

    public static byte[] buildScanQueryToBytes(ScanQuery scanQuery) {
        return buildScanQuery(scanQuery).toByteArray();
    }

    public static Search.ScanQuery buildScanQuery(ScanQuery scanQuery) {
        Search.ScanQuery.Builder builder = Search.ScanQuery.newBuilder();
        if (null != scanQuery.getLimit()) {
            builder.setLimit(scanQuery.getLimit());
        }
        if (null != scanQuery.getQuery()) {
            builder.setQuery(SearchQueryBuilder.buildQuery(scanQuery.getQuery()));
        }
        if (null != scanQuery.getToken()) {
            builder.setToken(ByteString.copyFrom(scanQuery.getToken()));
        }
        if (null != scanQuery.getMaxParallel()) {
            builder.setMaxParallel(scanQuery.getMaxParallel());
        }
        if (null != scanQuery.getCurrentParallelId()) {
            builder.setCurrentParallelId(scanQuery.getCurrentParallelId());
        }
        if (null != scanQuery.getAliveTime()) {
            builder.setAliveTime(scanQuery.getAliveTime());
        }
        return builder.build();
    }

    public static byte[] buildSearchRequestToBytes(SearchRequest request) {
        return buildSearchRequest(request).toByteArray();
    }

    public static Search.SearchRequest buildSearchRequest(SearchRequest request) {
        Search.SearchRequest.Builder builder = Search.SearchRequest.newBuilder();
        if (null != request.getTableName()) {
            builder.setTableName(request.getTableName());
        }
        if (null != request.getIndexName()) {
            builder.setIndexName(request.getIndexName());
        }
        if (request.getColumnsToGet() != null) {
            builder.setColumnsToGet(buildColumnsToGet(request.getColumnsToGet()));
        }
        if (request.getSearchQuery() != null) {
            builder.setSearchQuery(buildSearchQuery(request.getSearchQuery()).toByteString());
        }
        if (request.getRoutingValues() != null) {
            List<ByteString> routingValues = new ArrayList<ByteString>();
            for (PrimaryKey pk : request.getRoutingValues()) {
                try {
                    routingValues.add(ByteString.copyFrom(PlainBufferBuilder.buildPrimaryKeyWithHeader(pk)));
                } catch (IOException e) {
                    throw new ClientException("build plain buffer fail", e);
                }
            }
            builder.addAllRoutingValues(routingValues);
        }
        if (request.getTimeoutInMillisecond() > 0) {
            builder.setTimeoutMs(request.getTimeoutInMillisecond());
        }
        return builder.build();
    }

    public static byte[] buildParallelScanRequestToBytes(ParallelScanRequest request) {
        return buildParallelScanRequest(request).toByteArray();
    }

    public static Search.ParallelScanRequest buildParallelScanRequest(ParallelScanRequest parallelScanRequest) {
        Search.ParallelScanRequest.Builder builder = Search.ParallelScanRequest.newBuilder();
        if (null != parallelScanRequest.getTableName()) {
            builder.setTableName(parallelScanRequest.getTableName());
        }
        if (null != parallelScanRequest.getIndexName()) {
            builder.setIndexName(parallelScanRequest.getIndexName());
        }
        if (null != parallelScanRequest.getColumnsToGet()) {
            builder.setColumnsToGet(buildColumnsToGet(parallelScanRequest.getColumnsToGet()));
        }
        if (null != parallelScanRequest.getSessionId()) {
            builder.setSessionId(ByteString.copyFrom(parallelScanRequest.getSessionId()));
        }
        if (null != parallelScanRequest.getScanQuery()) {
            builder.setScanQuery(buildScanQuery(parallelScanRequest.getScanQuery()));
        }
        if (parallelScanRequest.getTimeoutInMillisecond() > 0) {
            builder.setTimeoutMs(parallelScanRequest.getTimeoutInMillisecond());
        }
        return builder.build();
    }

    public static Search.DateTimeUnit buildDateTimeUnit(DateTimeUnit unit) {
        switch (unit) {
            case YEAR:
                return Search.DateTimeUnit.YEAR;
            case QUARTER_YEAR:
                return Search.DateTimeUnit.QUARTER_YEAR;
            case MONTH:
                return Search.DateTimeUnit.MONTH;
            case WEEK:
                return Search.DateTimeUnit.WEEK;
            case DAY:
                return Search.DateTimeUnit.DAY;
            case HOUR:
                return Search.DateTimeUnit.HOUR;
            case MINUTE:
                return Search.DateTimeUnit.MINUTE;
            case SECOND:
                return Search.DateTimeUnit.SECOND;
            case MILLISECOND:
                return Search.DateTimeUnit.MILLISECOND;
            default:
                throw new IllegalArgumentException("Unknown DateTimeUnit: " + unit.name());
        }
    }

    public static Search.DateTimeValue buildDateTimeValue(DateTimeValue dateTimeValue) {
        Search.DateTimeValue.Builder builder = Search.DateTimeValue.newBuilder();
        if (dateTimeValue.getValue() != null) {
            builder.setValue(dateTimeValue.getValue());
        }
        if (dateTimeValue.getUnit() != null) {
            builder.setUnit(buildDateTimeUnit(dateTimeValue.getUnit()));
        }
        return builder.build();
    }
}
