package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class PlainBufferBlockRowIterator implements Iterator<Row> {

    private SyncClientInterface client;
    private BulkExportQueryCriteria parameter;

    private BulkExportResponse result;
    private Iterator<Row> rowsIter;

    public PlainBufferBlockRowIterator(SyncClientInterface client, BulkExportQueryCriteria parameter) {
        this.client = client;
        this.parameter = parameter;
        fetchData(buildRequest());
    }
    
    private BulkExportRequest buildRequest() {
        BulkExportQueryCriteria criteria = new BulkExportQueryCriteria(parameter.getTableName());
        if (parameter.hasSetFilter()) {
            criteria.setFilter(parameter.getFilter());
        }

        if (result == null) {
            criteria.setInclusiveStartPrimaryKey(parameter.getInclusiveStartPrimaryKey());
        } else {
            criteria.setInclusiveStartPrimaryKey(result.getNextStartPrimaryKey());
        }

        criteria.setExclusiveEndPrimaryKey(parameter.getExclusiveEndPrimaryKey());
        criteria.setDataBlockType(DataBlockType.DBT_PLAIN_BUFFER);

        criteria.addColumnsToGet(parameter.getColumnsToGet());

        if (parameter.hasSetFilter()) {
            criteria.setFilter(parameter.getFilter());
        }
        return new BulkExportRequest(criteria);
    }

    private void fetchData(BulkExportRequest request) {
        this.result = client.bulkExport(request);
        this.rowsIter = new PlainBufferBlockParser(result.getRows()).getRows().iterator();
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
        while (nextToken != null && !nextToken.isEmpty()) {
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
