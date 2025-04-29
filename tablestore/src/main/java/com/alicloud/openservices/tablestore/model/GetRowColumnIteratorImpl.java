package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.InternalClient;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class GetRowColumnIteratorImpl extends AbstractColumnIteratorImpl {

    private InternalClient internalClient;
    private GetRowRequest getRowRequest;
    private PrimaryKey primaryKey;
    private Iterator<Column> columnIterator;
    private byte[] token;
    private boolean complete = false;
    private CapacityUnit capacityUnit = new CapacityUnit(0, 0);

    public GetRowColumnIteratorImpl(InternalClient client, GetRowRequest getRowRequest) {
        this.internalClient = client;
        this.getRowRequest = getRowRequest;
    }

    public boolean isRowExistent() {
        while (!complete && (primaryKey == null)) {
            fetchData();
        }
        if (primaryKey != null) {
            return true;
        } else {
            return false;
        }
    }

    public void fetchData() {
        getRowRequest.getRowQueryCriteria().setToken(token);
        GetRowResponse getRowResponse = null;
        try {
            getRowResponse = internalClient.getRowInternal(getRowRequest, null).get();
        } catch (Exception ex) {
            throw new ClientException(ex);
        }
        capacityUnit.setReadCapacityUnit(capacityUnit.getReadCapacityUnit() + getRowResponse.getConsumedCapacity().getCapacityUnit().getReadCapacityUnit());
        capacityUnit.setWriteCapacityUnit(capacityUnit.getWriteCapacityUnit() + getRowResponse.getConsumedCapacity().getCapacityUnit().getWriteCapacityUnit());
        Row row = getRowResponse.getRow();
        if (row != null) {
            if (primaryKey == null) {
                primaryKey = row.getPrimaryKey();
            }
            columnIterator = Arrays.asList(row.getColumns()).iterator();
        }
        if (getRowResponse.hasNextToken()) {
            token = getRowResponse.getNextToken();
        } else {
            complete = true;
        }
    }

    @Override
    public boolean hasNext() {
        while (!complete && ((columnIterator == null) || (!columnIterator.hasNext()))) {
            fetchData();
        }
        if (columnIterator != null && columnIterator.hasNext()) {
            return true;
        }
        return false;
    }

    @Override
    public Column next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return columnIterator.next();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
