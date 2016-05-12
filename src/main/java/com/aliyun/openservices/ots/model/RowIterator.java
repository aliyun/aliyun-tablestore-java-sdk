/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.OTS;

public class RowIterator implements Iterator<Row> {

    private OTS ots;
    private RangeIteratorParameter parameter;
    
    private GetRangeResult result;
    private Iterator<Row> rowsIter;
    private int totalCount;
    private int bufferSize;
    private int rowsRead;

    public RowIterator(OTS ots, RangeIteratorParameter parameter) {
        this.ots = ots;
        this.parameter = parameter;
        this.rowsRead = 0;
        this.totalCount = parameter.getCount();
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
        criteria.setColumnsToGet(parameter.getColumnsToGet());
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
        if (parameter.getFilter() != null) {
            criteria.setFilter(parameter.getFilter());
        }
        return new GetRangeRequest(criteria);
    }

    private void fetchData(GetRangeRequest request) {
        GetRangeResult result = ots.getRange(request);
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
        RowPrimaryKey nextToken = result.getNextStartPrimaryKey();
        while (nextToken != null && !nextToken.getPrimaryKey().isEmpty() && totalCount != rowsRead) {
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
