package com.alicloud.openservices.tablestore.timestream.model.query;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.timestream.internal.TableMetaGenerator;
import com.alicloud.openservices.tablestore.timestream.model.*;
import com.alicloud.openservices.tablestore.timestream.model.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Multi-timeline query class
 */
public class MetaFilter {
    static Logger logger = LoggerFactory.getLogger(MetaFilter.class);

    private AsyncClient asyncClient;
    private String metaTableName;
    private String indexName;
    private Filter filter;
    private boolean returnAll = false;
    private List<String> attrToGet = null;
    private int limit = 100;
    private int offset = 0;
    private Sort sort = null;

    public MetaFilter(AsyncClient asyncClient,
                      String metaTableName, String indexName,
                      Filter filter) {
        this.asyncClient = asyncClient;
        this.metaTableName = metaTableName;
        this.indexName = indexName;
        this.filter = filter;
    }

    /**
     * The limit restriction for a single query of the multi-level index {@link SearchQuery#setLimit(Integer)}
     * @param limit
     * @return
     */
    public MetaFilter limit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * @param offset Pagination starting count
     * @return
     */
    public MetaFilter offset(int offset) {
        this.offset = offset;
        return this;
    }

	/**
     * Specifying the sorting condition will return the results sorted.
     * @param sorter Sorting rule
     * @return
     */
    public MetaFilter sort(Sorter sorter) {
        this.sort = new Sort(sorter.getSorter());
        return this;
    }

    /**
     * Set the attributes column names to be queried
     * @param columns
     * @return
     */
    public MetaFilter selectAttributes(String... columns) {
        if (this.returnAll) {
            throw new ClientException("returnAll has been set.");
        }
        this.attrToGet = Arrays.asList(columns);
        return this;
    }

    /**
     * Get the specified attributes column names for the query
     * @return
     */
    public List<String> getAttributesToSelect() {
        return this.attrToGet;
    }

    /**
     * Set to query the complete TimestreamMeta
     * @return
     */
    public MetaFilter returnAll() {
        if (this.attrToGet != null) {
            throw new ClientException("Attributes to select has been set.");
        }
        this.returnAll = true;
        return this;
    }

    /**
     * Whether to query the complete TimestreamMeta
     * @return
     */
    public boolean isReturnAll() {
        return this.returnAll;
    }

    /**
     * Get whether to query the complete TimestreamMeta
     * @return
     */
    public boolean getReturnAll() {
        return this.returnAll;
    }

    private TimestreamMetaIterator fetchMeta(List<String> colsToGet) {
        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setLimit(limit);
        searchQuery.setOffset(offset);
        searchQuery.setGetTotalCount(true);
        if (filter == null) {
            searchQuery.setQuery(new MatchAllQuery());
        } else {
            searchQuery.setQuery(filter.getQuery());
        }
        if (sort != null) {
            searchQuery.setSort(sort);
        }

        SearchRequest request = new SearchRequest(metaTableName, indexName, searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        if (colsToGet != null) {
            columnsToGet.setColumns(colsToGet);
        } else {
            columnsToGet.setReturnAll(true);
        }
        request.setColumnsToGet(columnsToGet);

        return new TimestreamMetaIterator(asyncClient, request);
    }

    /**
     * Query
     * @return
     */
    public TimestreamMetaIterator fetchAll() {
        ArrayList<String> colsToGet = null;
        if (this.returnAll) {
            // pass
        } else if (this.attrToGet != null){
            colsToGet = new ArrayList<String>();
            colsToGet.addAll(this.attrToGet);
            colsToGet.add(TableMetaGenerator.CN_TAMESTAMP_NAME);
        } else {
            colsToGet = new ArrayList<String>();
            colsToGet.add(TableMetaGenerator.CN_TAMESTAMP_NAME);
        }
        return fetchMeta(colsToGet);
    }
}
