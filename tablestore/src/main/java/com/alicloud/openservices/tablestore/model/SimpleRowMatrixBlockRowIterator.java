package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Row iterator for SimleRowMatrixBlock
 */
public class SimpleRowMatrixBlockRowIterator implements Iterator<Long> {

    private SyncClientInterface client;
    private BulkExportQueryCriteria parameter;

    private BulkExportResponse result;
    private SimpleRowMatrixBlockParser block;
    private Map<String, Integer> fieldNames;
    private long index = 0;
    private long completedBytes;

    // if true verify field names of first response the same as the following responses
    private boolean verifyFieldNames;

    public SimpleRowMatrixBlockRowIterator(SyncClientInterface client, BulkExportQueryCriteria parameter, boolean verifyFieldNames) {
        this.client = client;
        this.parameter = parameter;
        this.verifyFieldNames = verifyFieldNames;
        this.completedBytes = 0;
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
        criteria.setDataBlockType(DataBlockType.DBT_SIMPLE_ROW_MATRIX);

        criteria.addColumnsToGet(parameter.getColumnsToGet());

        if (parameter.hasSetFilter()) {
            criteria.setFilter(parameter.getFilter());
        }

        return new BulkExportRequest(criteria);
    }

    private void fetchData(BulkExportRequest request) {
        this.result = client.bulkExport(request);
        this.completedBytes += this.result.getBodyBytes();
        this.block = new SimpleRowMatrixBlockParser(this.result.getRows());

        if (fieldNames == null) {
            String[] fns = block.parseFieldNames();
            fieldNames = new HashMap<String, Integer>(fns.length);
            for (int i = 0; i < fns.length; i++) {
                this.fieldNames.put(fns[i], i);
            }
        } else if (verifyFieldNames) {
            String[] fns = block.parseFieldNames();
            if (fns.length != fieldNames.size()) {
                throw new ClientException("Inconsistent field name count");
            }
            for (int i = 0; i < fns.length; i++) {
                Integer idx = fieldNames.get(fns[i]);
                if (idx == null || idx != i) {
                    throw new ClientException("Inconsistent field name:" + fns[i]);
                }
            }
        }
    }

    private boolean isBufferHasMoreData() {
        return block != null && block.hasNext();
    }

    private long getNextFromBuffer() {
        block.next();
        return index++;
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
    public Long next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        return getNextFromBuffer();
    }

    public int locate(String fieldName) {
        Integer x = fieldNames.get(fieldName);
        if (x == null) {
            throw new ClientException("Can't find the field by name:" + fieldName);
        }
        return x;
    }

    // high-level interface but will generate many objects
    public Row getRow() {
        return block.getRow();
    }

    public boolean getBoolean(String fieldName) {
        int idx = locate(fieldName);
        return block.getBoolean(idx);
    }

    public boolean getBoolean(int index) {
        return block.getBoolean(index);
    }

    public String getString(String fieldName) {
        int idx = locate(fieldName);
        return block.getString(idx);
    }

    public String getString(int index) {
        return block.getString(index);
    }

    public long getLong(String fieldName) {
        int idx = locate(fieldName);
        return block.getLong(idx);
    }

    public long getLong(int index) {
        return block.getLong(index);
    }

    public double getDouble(String fieldName) {
        int idx = locate(fieldName);
        return block.getDouble(idx);
    }

    public double getDouble(int index) {
        return block.getDouble(index);
    }

    public byte[] getBinary(String fieldName) {
        int idx = locate(fieldName);
        return block.getBinary(idx);
    }

    public byte[] getBinary(int index) {
        return block.getBinary(index);
    }

    public boolean isNull(String fieldName) {
        int idx = locate(fieldName);
        return block.isNull(idx);
    }

    public boolean isNull(int index) {
        return block.isNull(index);
    }

    public Object getObject(String fieldName) {
        int idx = locate(fieldName);
        return block.getObject(idx);
    }

    public Object getObject(int index) {
        return block.getObject(index);
    }

    public ColumnType getFieldType(String fieldName) {
        int idx = locate(fieldName);
        return block.getColumnType(idx);
    }

    public ColumnType getFieldType(int index) {
        return block.getColumnType(index);
    }

    @Override
    public void remove() {
        throw new ClientException("SimpleRowMatrixBlockRowIterator do not support remove().");
    }

    public long getCompletedBytes() {
        return this.completedBytes;
    }
}

