package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.ExtensionRequest;
import com.google.gson.GsonBuilder;
import java.util.Arrays;
import java.util.List;

import com.alicloud.openservices.tablestore.model.ComputeSplitsResponse;
import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.search.SearchRequest.ColumnsToGet;

/**
 * Scan data from search index.
 */
public class ParallelScanRequest extends ExtensionRequest {

    /**
     * the name of your table.
     */
    private String tableName;

    /**
     * SearchIndex index name.
     */
    private String indexName;

    /**
     * Query criteria.
     */
    private ScanQuery scanQuery;

    /**
     * Specify which property columns need to return. Recommend returning only what you need.
     */
    private ColumnsToGet columnsToGet;

    /**
     * {@link ParallelScanRequest} establishes a link to the server with this sessionId.
     * sessionId can get from {@link ComputeSplitsResponse#getSessionId}.
     */
    private byte[] sessionId;

    /**
     * Request-level parallel scan timeout in millisecond
     */
    private int timeoutInMillisecond = -1;

    public String getRequestInfo(boolean prettyFormat) {
        GsonBuilder builder = new GsonBuilder()
            .disableHtmlEscaping()
            .disableInnerClassSerialization()
            .serializeNulls()
            .serializeSpecialFloatingPointValues()
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


    public ParallelScanRequest() {
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public String getTableName() {
        return tableName;
    }

    public ParallelScanRequest setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getIndexName() {
        return indexName;
    }

    public ParallelScanRequest setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public ScanQuery getScanQuery() {
        return scanQuery;
    }

    public ParallelScanRequest setScanQuery(ScanQuery scanQuery) {
        this.scanQuery = scanQuery;
        return this;
    }

    public ColumnsToGet getColumnsToGet() {
        return columnsToGet;
    }

    public ParallelScanRequest setColumnsToGet(ColumnsToGet columnsToGet) {
        this.columnsToGet = columnsToGet;
        return this;
    }

    public byte[] getSessionId() {
        return sessionId;
    }

    public ParallelScanRequest setSessionId(byte[] sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public int getTimeoutInMillisecond() {
        return this.timeoutInMillisecond;
    }

    public ParallelScanRequest setTimeoutInMillisecond(int timeoutInMillisecond) {
        this.timeoutInMillisecond = timeoutInMillisecond;
        return this;
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_PARALLEL_SCAN;
    }

    private ParallelScanRequest(Builder builder) {
        setTableName(builder.tableName);
        setIndexName(builder.indexName);
        setScanQuery(builder.scanQuery);
        setColumnsToGet(builder.columnsToGet);
        setSessionId(builder.sessionId);
        setTimeoutInMillisecond(builder.timeoutInMillisecond);
    }

    public static final class Builder {
        private String tableName;
        private String indexName;
        private ScanQuery scanQuery;
        private ColumnsToGet columnsToGet;
        private byte[] sessionId;
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

        public Builder scanQuery(ScanQuery scanQuery) {
            this.scanQuery = scanQuery;
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
         * All property columns which had created searchIndex need to return.
         */
        public Builder returnAllColumnsFromIndex(boolean returnAllIndex) {
            if (this.columnsToGet == null) {
                this.columnsToGet = new ColumnsToGet();
            }
            this.columnsToGet.setReturnAllFromIndex(returnAllIndex);
            return this;
        }

        public Builder sessionId(byte[] sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public Builder timeout(int timeoutInMillisecond) {
            this.timeoutInMillisecond = timeoutInMillisecond;
            return this;
        }

        public ParallelScanRequest build() {
            return new ParallelScanRequest(this);
        }
    }
}
