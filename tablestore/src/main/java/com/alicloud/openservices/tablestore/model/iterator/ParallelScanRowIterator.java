package com.alicloud.openservices.tablestore.model.iterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.ParallelScanResponse;

public class ParallelScanRowIterator implements RowIterator {

    private SyncClientInterface client;
    private Iterator<Row> rowsIterator;
    private ParallelScanRequest parallelScanRequest;
    private ParallelScanResponse parallelScanResponse;
    private long completedBytes;

    public ParallelScanRowIterator(SyncClientInterface syncClient, ParallelScanRequest parallelScanRequest) {
        this.parallelScanRequest = parallelScanRequest;
        this.client = syncClient;
        if (null != parallelScanRequest.getScanQuery().getToken()) {
            throw new IllegalArgumentException("ScanQuery's token must be null when initializing the ParallelScanRowIterator.");
        }
        fetchData();
    }

    private void fetchData() {
        if (null != parallelScanResponse) {
            if (null == parallelScanResponse.getNextToken()) {
                return;
            } else {
                parallelScanRequest.getScanQuery().setToken(parallelScanResponse.getNextToken());
            }
        }
        parallelScanResponse = this.client.parallelScan(parallelScanRequest);
        completedBytes += parallelScanResponse.getBodyBytes();
        rowsIterator = parallelScanResponse.getRows().iterator();
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
        throw new ClientException("ParallelScanRowIterator do not support getTotalCount().");
    }

    public long getCompletedBytes() {
        return this.completedBytes;
    }
}
