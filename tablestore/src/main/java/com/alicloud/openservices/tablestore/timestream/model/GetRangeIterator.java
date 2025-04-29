package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Row;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class GetRangeIterator implements Iterator<Row> {
    private AsyncClient asyncClient;
    private GetRangeRequest request;
    private GetRangeResponse response = null;
    private Iterator<Row> rowIterator;

    public GetRangeIterator(AsyncClient asyncClient, GetRangeRequest request) {
        this.asyncClient = asyncClient;
        this.request = request;

        fetchData(request);
    }

    private void fetchData(GetRangeRequest request) {
        try {
            response = this.asyncClient.getRange(request, null).get();
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
        PrimaryKey token = response.getNextStartPrimaryKey();

        while (token != null) {
            request.getRangeRowQueryCriteria().setInclusiveStartPrimaryKey(token);
            fetchData(request);
            if (isBufferHasData()) {
                return true;
            }
            token = response.getNextStartPrimaryKey();
        }
        return false;
    }

    public Row next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        return rowIterator.next();
    }

    public void remove() {
        throw new RuntimeException("GetRangeIterator do not support remove().");
    }
}
