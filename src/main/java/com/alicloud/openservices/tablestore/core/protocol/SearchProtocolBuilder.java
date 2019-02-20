package com.alicloud.openservices.tablestore.core.protocol;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.search.*;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SearchProtocolBuilder {

    static final int DEFAULT_NUMBER_OF_SHARDS = 1;

    public static Search.FieldType buildFieldType(FieldType fieldType) {
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
            default:
                throw new IllegalArgumentException("Unknown fieldType: " + fieldType.name());
        }
    }

    public static Search.IndexOptions buildIndexOptions(IndexOptions indexOptions) {
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

    public static Search.FieldSchema buildFieldSchema(FieldSchema fieldSchema) {
        Search.FieldSchema.Builder builder = Search.FieldSchema.newBuilder();
        builder.setFieldName(fieldSchema.getFieldName());
        builder.setFieldType(buildFieldType(fieldSchema.getFieldType()));
        if (fieldSchema.getFieldType() != FieldType.NESTED) {
            if (fieldSchema.isIndex() != null) {
                builder.setIndex(fieldSchema.isIndex());
            }
            if (fieldSchema.isEnableSortAndAgg() != null) {
                builder.setDocValues(fieldSchema.isEnableSortAndAgg());
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
        }
        if (fieldSchema.getIndexOptions() != null) {
            builder.setIndexOptions(buildIndexOptions(fieldSchema.getIndexOptions()));
        }
        if (fieldSchema.getAnalyzer() != null) {
            builder.setAnalyzer(fieldSchema.getAnalyzer().toString());
        }
        if (fieldSchema.getSubFieldSchemas() != null) {
            for (FieldSchema subSchema : fieldSchema.getSubFieldSchemas()) {
                builder.addFieldSchemas(buildFieldSchema(subSchema));
            }
        }
        return builder.build();
    }

    public static Search.IndexSetting buildIndexSetting(IndexSetting indexSetting) {
        Search.IndexSetting.Builder builder = Search.IndexSetting.newBuilder();
        builder.setNumberOfShards(DEFAULT_NUMBER_OF_SHARDS);
        if (indexSetting.getRoutingFields() != null) {
            builder.addAllRoutingFields(indexSetting.getRoutingFields());
        }
        return builder.build();
    }

    public static Search.IndexSchema buildIndexSchema(IndexSchema indexSchema) {
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

    public static Search.DescribeSearchIndexRequest buildDescribeSearchIndexRequest(DescribeSearchIndexRequest request) {
        Search.DescribeSearchIndexRequest.Builder builder = Search.DescribeSearchIndexRequest.newBuilder();
        builder.setTableName(request.getTableName());
        builder.setIndexName(request.getIndexName());
        return builder.build();
    }

    public static Search.ColumnsToGet buildColumnsToGet(SearchRequest.ColumnsToGet columnsToGet) {
        Search.ColumnsToGet.Builder builder = Search.ColumnsToGet.newBuilder();
        if (columnsToGet.isReturnAll()) {
            builder.setReturnType(Search.ColumnReturnType.RETURN_ALL);
        } else if (columnsToGet.getColumns().size() > 0) {
            builder.setReturnType(Search.ColumnReturnType.RETURN_SPECIFIED);
            builder.addAllColumnNames(columnsToGet.getColumns());
        } else {
            builder.setReturnType(Search.ColumnReturnType.RETURN_NONE);
        }
        return builder.build();
    }

    public static Search.Collapse buildCollapse(Collapse collapse) {
        Search.Collapse.Builder builder = Search.Collapse.newBuilder();
        builder.setFieldName(collapse.getFieldName());
        return builder.build();
    }

    public static Search.SearchQuery buildSearchQuery(SearchQuery searchQuery) {
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
        if (searchQuery.getSort() != null) {
            builder.setSort(SearchSortBuilder.buildSort(searchQuery.getSort()));
        }
        if (searchQuery.getCollapse() != null) {
            builder.setCollapse(buildCollapse(searchQuery.getCollapse()));
        }
        builder.setGetTotalCount(searchQuery.isGetTotalCount());
        if (searchQuery.getToken() != null) {
            builder.setToken(ByteString.copyFrom(searchQuery.getToken()));
        }
        return builder.build();
    }

    public static Search.SearchRequest buildSearchRequest(SearchRequest request) {
        Search.SearchRequest.Builder builder = Search.SearchRequest.newBuilder();
        builder.setTableName(request.getTableName());
        builder.setIndexName(request.getIndexName());
        if (request.getColumnsToGet() != null) {
            builder.setColumnsToGet(buildColumnsToGet(request.getColumnsToGet()));
        }
        builder.setSearchQuery(buildSearchQuery(request.getSearchQuery()).toByteString());
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
        return builder.build();
    }
}
