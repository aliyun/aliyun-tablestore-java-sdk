package com.alicloud.openservices.tablestore.model.iterator;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class BulkExportIterator implements RowIterator {

    private SyncClientInterface client;
    private RangeIteratorParameter parameter;

    private BulkExportResponse result;
    private Iterator<Row> rowsIter;
    private int totalCount;
    private int bufferSize;
    private int rowsRead;

    public BulkExportIterator(SyncClientInterface client, RangeIteratorParameter parameter) {
        this.client = client;
        this.parameter = parameter;
        this.rowsRead = 0;
        this.totalCount = parameter.getMaxCount();
        this.bufferSize = parameter.getBufferSize();

        if (bufferSize == -1) {
            bufferSize = totalCount;
        } else if (totalCount != -1 && bufferSize > totalCount) {
            bufferSize = totalCount;
        }

        fetchData(buildRequest());
    }

    private BulkExportRequest buildRequest() {
        BulkExportQueryCriteria criteria = new BulkExportQueryCriteria(parameter.getTableName());
        criteria.addColumnsToGet(parameter.getColumnsToGet());

        if (parameter.hasSetFilter()) {
            criteria.setFilter(parameter.getFilter());
        }

        if (result == null) {
            criteria.setInclusiveStartPrimaryKey(parameter.getInclusiveStartPrimaryKey());
        } else {
            criteria.setInclusiveStartPrimaryKey(result.getNextStartPrimaryKey());
        }

        criteria.setExclusiveEndPrimaryKey(parameter.getExclusiveEndPrimaryKey());
        if (bufferSize > 0) {
            if (totalCount != -1 && totalCount - rowsRead < bufferSize) {
                //criteria.setLimit(totalCount - rowsRead);
            } else {
                //criteria.setLimit(bufferSize);
            }
        }
        return new BulkExportRequest(criteria);
    }

    private void fetchData(BulkExportRequest request) {
        BulkExportResponse result = client.bulkExport(request);
        this.result = result;
        SimpleRowMatrixBlockParser parser = new SimpleRowMatrixBlockParser(result.getRows());
        this.rowsIter = parser.getRows().iterator();
        this.rowsRead += parser.getRows().size();
    }

    private boolean isBufferHasMoreData() {
        return rowsIter.hasNext();
    }

    private Row getNextFromBuffer() {
        return rowsIter.next();
    }

    @Override
    public boolean hasNext() {
        // has data in buffer
        if (isBufferHasMoreData()) {
            return true;
        }

        // need to send one more request
        PrimaryKey nextToken = result.getNextStartPrimaryKey();
        while (nextToken != null && !nextToken.isEmpty() && totalCount != rowsRead) {
            fetchData(buildRequest());
            if (isBufferHasMoreData()) {
                return true;
            }
            nextToken = result.getNextStartPrimaryKey();
        }

        return false;
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

    public long getTotalCount() {
        return totalCount;
    }
}

