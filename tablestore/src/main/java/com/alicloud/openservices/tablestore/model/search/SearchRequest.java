package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.ExtensionRequest;
import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;

/**
 * SearchIndex's Request
 */
public class SearchRequest extends ExtensionRequest {

    public SearchRequest() {
    }

    public SearchRequest(String tableName, String indexName, SearchQuery queryBase) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.searchQuery = queryBase;
    }

    /**
     * TableStore's table name
     */
    private String tableName;

    /**
     * SearchIndex index name
     */
    private String indexName;

    /**
     * Query criteria. {@link SearchQuery}
     */
    private SearchQuery searchQuery;

    /**
     * Specify which property columns need to return. Recommend returning only what you need.
     */
    private ColumnsToGet columnsToGet;

    /**
     * Request-level search timeout in millisecond
     */
    private int timeoutInMillisecond = -1;

    /**
     * Routing field.
     * <p>The default is empty, which is not required in most scenarios. If a custom route is used, you can specify the route field.</p>
     * <p>Note: <b>advanced features.</b> If you need to know or use, please submit a ticket or contact the developer.</p>
     */
    private List<PrimaryKey> routingValues;


    public static class ColumnsToGet {
        private List<String> columns = new ArrayList<String>();
        private boolean returnAll = false;
        private boolean returnAllFromIndex = false;

        public List<String> getColumns() {
            return columns;
        }

        public void setColumns(List<String> columns) {
            this.columns = columns;
        }

        public boolean isReturnAll() {
            return returnAll;
        }

        public void setReturnAll(boolean returnAll) {
            if (returnAll && returnAllFromIndex) {
                throw new IllegalArgumentException("The parameter returnAll and returnAllFromIndex should not all be true.");
            }
            this.returnAll = returnAll;
        }

        public boolean isReturnAllFromIndex() {
            return returnAllFromIndex;
        }

        public void setReturnAllFromIndex(boolean returnAllFromIndex) {
            if (returnAll && returnAllFromIndex) {
                throw new IllegalArgumentException("The parameter returnAll and returnAllFromIndex should not all be true.");
            }
            this.returnAllFromIndex = returnAllFromIndex;
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public SearchRequest setSearchQuery(SearchQuery searchQuery) {
        this.searchQuery = searchQuery;
        return this;
    }

    public  SearchQuery getSearchQuery() {
        return searchQuery;
    }

    public ColumnsToGet getColumnsToGet() {
        return columnsToGet;
    }

    public void setColumnsToGet(ColumnsToGet columnsToGet) {
        this.columnsToGet = columnsToGet;
    }

    public List<PrimaryKey> getRoutingValues() {
        return routingValues;
    }

    public void setRoutingValues(List<PrimaryKey> routingValues) {
        this.routingValues = routingValues;
    }

    public int getTimeoutInMillisecond() {
        return timeoutInMillisecond;
    }

    public void setTimeoutInMillisecond(int timeoutInMillisecond) {
        this.timeoutInMillisecond = timeoutInMillisecond;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_SEARCH;
    }

    public String getRequestInfo(boolean prettyFormat) {
        GsonBuilder builder = new GsonBuilder()
            .disableHtmlEscaping()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
            .setExclusionStrategies(new ExclusionStrategy() {
                @Override
                public boolean shouldSkipField(FieldAttributes f) {
                    return f.getName().equals("dataSize")
                        && f.getDeclaredClass().getName().equals("int")
                        && f.getDeclaringClass().equals(ColumnValue.class);
                }

                @Override
                public boolean shouldSkipClass(Class<?> aClass) {
                    return false;
                }
            })
            .enableComplexMapKeySerialization();
        if (prettyFormat) {
            return builder.setPrettyPrinting().create().toJson(this);
        } else {
            return builder.create().toJson(this);
        }
    }

    public void printRequestInfo() {
        System.out.println(getRequestInfo(true));
    }

    private SearchRequest(Builder builder) {
        setTableName(builder.tableName);
        setIndexName(builder.indexName);
        setSearchQuery(builder.searchQuery);
        setColumnsToGet(builder.columnsToGet);
        setRoutingValues(builder.routingValues);
        setTimeoutInMillisecond(builder.timeoutInMillisecond);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String tableName;
        private String indexName;
        private SearchQuery searchQuery;
        private ColumnsToGet columnsToGet;
        private List<PrimaryKey> routingValues;
        private int timeoutInMillisecond = -1;

        private Builder() {}

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder searchQuery(SearchQuery searchQuery) {
            this.searchQuery = searchQuery;
            return this;
        }

        /**
         * Specify which property columns need to return. Recommend returning only what you need.
         */
        public Builder addColumnsToGet(String... columnsToGet) {
            if (this.columnsToGet == null) {
                this.columnsToGet = new ColumnsToGet();
            }
            this.columnsToGet.getColumns().addAll(Arrays.asList(columnsToGet));
            return this;
        }

        /**
         * Specify which property columns need to return. Recommend returning only what you need.
         */
        public Builder addColumnsToGet(List<String> columnsToGetList) {
            if (this.columnsToGet == null) {
                this.columnsToGet = new ColumnsToGet();
            }
            this.columnsToGet.getColumns().addAll(columnsToGetList);
            return this;
        }

        /**
         * All property columns need to return.
         */
        public Builder returnAllColumns(boolean returnAll) {
            if (this.columnsToGet == null) {
                this.columnsToGet = new ColumnsToGet();
            }
            this.columnsToGet.setReturnAll(returnAll);
            return this;
        }

        /**
         * All property columns which had created searchIndex need to return.
         */
        public Builder returnAllColumnsFromIndex(boolean returnAllIndex) {
            if (this.columnsToGet == null) {
                this.columnsToGet = new ColumnsToGet();
            }
            this.columnsToGet.setReturnAllFromIndex(returnAllIndex);
            return this;
        }

        /**
         * Routing field.
         * <p>The default is empty, which is not required in most scenarios. If a custom route is used, you can specify the route field.</p>
         * <p>Note: <b>advanced features.</b> If you need to know or use, please submit a ticket or contact the developer.</p>
         */
        public Builder addRoutingValue(PrimaryKeyBuilder primaryKeyBuilder) {
            if (routingValues == null) {
                routingValues = new ArrayList<PrimaryKey>();
            }
            this.routingValues.add(primaryKeyBuilder.build());
            return this;
        }

        /**
         * Routing field.
         * <p>The default is empty, which is not required in most scenarios. If a custom route is used, you can specify the route field.</p>
         * <p>Note: <b>advanced features.</b> If you need to know or use, please submit a ticket or contact the developer.</p>
         */
        public Builder addRoutingValue(PrimaryKey primaryKey) {
            if (routingValues == null) {
                routingValues = new ArrayList<PrimaryKey>();
            }
            this.routingValues.add(primaryKey);
            return this;
        }

        /**
         * Routing field.
         * <p>The default is empty, which is not required in most scenarios. If a custom route is used, you can specify the route field.</p>
         * <p>Note: <b>advanced features.</b> If you need to know or use, please submit a ticket or contact the developer.</p>
         */
        public Builder addRoutingValues(List<PrimaryKey> primaryKeys) {
            if (routingValues == null) {
                routingValues = new ArrayList<PrimaryKey>();
            }
            this.routingValues.addAll(primaryKeys);
            return this;
        }

        public Builder timeout(int timeoutInMillisecond) {
            if (timeoutInMillisecond > 0) {
                this.timeoutInMillisecond = timeoutInMillisecond;
            }
            return this;
        }

        public SearchRequest build() {
            return new SearchRequest(this);
        }
    }
}
