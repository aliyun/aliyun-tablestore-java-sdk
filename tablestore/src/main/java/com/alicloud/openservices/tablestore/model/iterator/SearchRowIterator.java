package com.alicloud.openservices.tablestore.model.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;

public class SearchRowIterator implements RowIterator {

    private static int DEFAULT_PAGE_LIMIT = 100;

    private SyncClientInterface client;
    private Iterator<Row> rowsIterator;
    private SearchRequest searchRequest;
    private SearchResponse searchResponse;
    private long completedBytes;

    public SearchRowIterator(SyncClientInterface syncClient, SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
        this.client = syncClient;
        this.completedBytes = 0;
        fetchData();
    }

    private void fetchData() {
        if (null != searchResponse) {
            if (null == searchResponse.getNextToken()) {
                return;
            } else {
                searchRequest.getSearchQuery().setToken(searchResponse.getNextToken());
            }
        }
        if (searchRequest.getSearchQuery().getLimit() == null) {
            searchRequest.getSearchQuery().setLimit(DEFAULT_PAGE_LIMIT);
        }
        searchResponse = this.client.search(searchRequest);
        completedBytes += searchResponse.getBodyBytes();
        if (searchResponse.isAllSuccess()) {
            rowsIterator = searchResponse.getRows().iterator();
        } else {
            throw new RuntimeException("not all success");
        }
    }


    private boolean isBufferHasMoreData() {
        return rowsIterator.hasNext();
    }

    private Row getNextFromBuffer() {
        return rowsIterator.next();
    }

    @Override
    public boolean hasNext() {
        // has data in buffer
        if (isBufferHasMoreData()) {
            return true;
        }
        fetchData();
        return isBufferHasMoreData();
    }

    @Override
    public Row next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return getNextFromBuffer();
    }

    @Override
    public void remove() {
        throw new ClientException("RowIterator do not support remove().");
    }

    @Override
    public long getTotalCount() {
        return searchResponse == null ? -1 : searchResponse.getTotalCount();
    }

    public long getCompletedBytes() {
        return completedBytes;
    }
}
