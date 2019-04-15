package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.Row;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class GetRowIterator implements Iterator<Row> {
    private AsyncClient asyncClient;
    private GetRowRequest request;
    private GetRowResponse response;
    private boolean getNext = false;

    public GetRowIterator(AsyncClient asyncClient, GetRowRequest request) {
        this.asyncClient = asyncClient;
        this.request = request;
        fetchData();
    }

    private void fetchData() {
        try {
            response = asyncClient.getRow(request, null).get();
        } catch (InterruptedException e) {
            throw new ClientException(String.format("The thread was interrupted: %s", e.getMessage()));
        } catch (ExecutionException e) {
            throw new ClientException("The thread was aborted", e);
        }
    }

    public boolean hasNext() {
        if (!getNext) {
            return response.getRow() != null;
        } else {
            return false;
        }
    }

    public Row next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        getNext = true;
        return response.getRow();
    }

    public void remove() {
        throw new RuntimeException("GetRangeIterator do not support remove().");
    }
}
