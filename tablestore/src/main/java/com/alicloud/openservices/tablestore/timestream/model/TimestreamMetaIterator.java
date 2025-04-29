package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

public class TimestreamMetaIterator implements Iterator<TimestreamMeta> {
    private static Logger logger = LoggerFactory.getLogger(TimestreamMetaIterator.class);

    private AsyncClient asyncClient;
    private SearchRequest request;
    private SearchResponse response;
    private Iterator<Row> rowIterator;

    public TimestreamMetaIterator(AsyncClient asyncClient, SearchRequest request) {
        this.asyncClient = asyncClient;
        this.request = request;

        fetchData(request);
    }

    private void fetchData(SearchRequest request) {
        try {
            response = this.asyncClient.search(request, null).get();
            logger.debug("fetchData response -> TraceId: " + response.getTraceId() + ", RequestId: " + response.getRequestId());
        } catch (InterruptedException e) {
            throw new ClientException(String.format("The thread was interrupted: %s", e.getMessage()));
        } catch (ExecutionException e) {
            throw new ClientException("The thread was aborted", e);
        }
        rowIterator = response.getRows().iterator();
    }

    private boolean isBufferHasData() {
        return this.rowIterator.hasNext();
    }

    public boolean hasNext() {
        if (isBufferHasData()) {
            return true;
        }
        byte[] token = response.getNextToken();

        while (token != null) {
            request.getSearchQuery().setToken(response.getNextToken());
            request.getSearchQuery().setOffset(0);
            fetchData(request);
            if (isBufferHasData()) {
                return true;
            }
            token = response.getNextToken();
        }
        return false;
    }

    public TimestreamMeta next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        return TimestreamMeta.newInstance(rowIterator.next());
    }

    public void remove() {
        throw new RuntimeException("TimestreamMetaIterator do not support remove().");
    }

    public long getTotalCount() {
        return this.response.getTotalCount();
    }

    protected SearchResponse getResponse() {
        return response;
    }
}
