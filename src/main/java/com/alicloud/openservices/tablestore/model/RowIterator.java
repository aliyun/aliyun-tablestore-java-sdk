package com.alicloud.openservices.tablestore.model;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.SyncClientInterface;

public class RowIterator implements Iterator<Row> {

    private SyncClientInterface client;
    private RangeIteratorParameter parameter;
    
    private GetRangeResponse result;
    private Iterator<Row> rowsIter;
    private int totalCount;
    private int bufferSize;
    private int rowsRead;

    public RowIterator(SyncClientInterface client, RangeIteratorParameter parameter) {
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
    
    private GetRangeRequest buildRequest() {
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(parameter.getTableName());
        criteria.setDirection(parameter.getDirection());
        criteria.addColumnsToGet(parameter.getColumnsToGet());
        if (parameter.hasSetCacheBlock()) {
            criteria.setCacheBlocks(parameter.getCacheBlocks());
        }

        if (parameter.hasSetMaxVersions()) {
            criteria.setMaxVersions(parameter.getMaxVersions());
        }

        if (parameter.hasSetTimeRange()) {
            criteria.setTimeRange(parameter.getTimeRange());
        }

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
                criteria.setLimit(totalCount - rowsRead);
            } else {
                criteria.setLimit(bufferSize);
            }
        }
        return new GetRangeRequest(criteria);
    }

    private void fetchData(GetRangeRequest request) {
    	GetRangeResponse result = client.getRange(request);
        this.result = result;
        this.rowsIter = result.getRows().iterator();
        this.rowsRead += result.getRows().size();
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

}
