package com.alicloud.openservices.tablestore.model.search;

import com.alicloud.openservices.tablestore.model.OperationNames;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Request;

import java.util.List;


/**
 * SearchIndex的搜索的Request
 */
public class SearchRequest implements Request {

    public static class ColumnsToGet {
        private List<String> columns;
        private boolean returnAll;

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
            this.returnAll = returnAll;
        }
    }

    public SearchRequest(String tableName, String indexName, SearchQuery searchQuery) {
        this.tableName = tableName;
        this.indexName = indexName;
        this.searchQuery = searchQuery;
    }

    /**
     * TableStore的表名
     */
    private String tableName;
    /**
     * SearchIndex中的index名
     */
    private String indexName;
    /**
     * 查询语句，具体参数详见{@link SearchQuery}
     */
    private SearchQuery searchQuery;
    /**
     * 指定哪些属性列需要返回
     * <p>如果SearchIndex中的属性列太多，而只想要某些属性列，则可以减少网络传输的数据量，提高响应速度</p>
     */
    private ColumnsToGet columnsToGet;
    /**
     * 路由字段
     * 默认为空，大多数场景下不需要使用该值。如果使用了自定义路由，可以指定路由字段。
     * <p>注意：<b>高级特性</b>。如需了解或使用请提工单或联系开发人员</p>
     */
    private List<PrimaryKey> routingValues;

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

    public SearchQuery getSearchQuery() {
        return searchQuery;
    }

    public void setSearchQuery(SearchQuery searchQuery) {
        this.searchQuery = searchQuery;
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

    @Override
    public String getOperationName() {
        return OperationNames.OP_SEARCH;
    }
}
