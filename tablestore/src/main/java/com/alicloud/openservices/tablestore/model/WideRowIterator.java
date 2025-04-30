package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.InternalClient;
import com.alicloud.openservices.tablestore.core.utils.Pair;

import java.util.*;

public class WideRowIterator implements Iterator<WideColumnIterator> {

    private InternalClient internalClient;
    private RangeIteratorParameter parameter;
    private PrimaryKey nextStartPrimaryKey;
    private byte[] nextToken;
    private int totalCount;
    private int bufferSize;
    private int rowsRead = 0;
    private RowRangeColumnIteratorImpl columnIterator;
    private Iterator<Pair<Row, Boolean> > rowIterator;

    public WideRowIterator(InternalClient client, RangeIteratorParameter rangeIteratorParameter) {
        this.internalClient = client;
        this.parameter = rangeIteratorParameter;
        this.totalCount = rangeIteratorParameter.getMaxCount();
        this.bufferSize = rangeIteratorParameter.getBufferSize();
        if (this.bufferSize == -1) {
            this.bufferSize = this.totalCount;
        } else if (totalCount != -1 && bufferSize > totalCount) {
            bufferSize = totalCount;
        }
        this.columnIterator = new RowRangeColumnIteratorImpl(this, null, new ArrayList<Column>().iterator(), true);
        this.nextStartPrimaryKey = this.parameter.getInclusiveStartPrimaryKey();
    }

    private GetRangeRequest buildRequest() {
        RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(parameter.getTableName());
        parameter.copyTo(criteria);
        criteria.setDirection(parameter.getDirection());
        criteria.setInclusiveStartPrimaryKey(nextStartPrimaryKey);
        criteria.setExclusiveEndPrimaryKey(parameter.getExclusiveEndPrimaryKey());
        criteria.setToken(nextToken);
        if (bufferSize > 0) {
            if (totalCount != -1 && totalCount - rowsRead < bufferSize) {
                criteria.setLimit(totalCount - rowsRead);
            } else {
                criteria.setLimit(bufferSize);
            }
        }
        return new GetRangeRequest(criteria);
    }

    void fetchData() {
        GetRangeResponse result = null;
        try {
            result = internalClient.getRange(buildRequest(), null).get();
        } catch (Exception e) {
            throw new ClientException(e);
        }

        int idx = 0;
        List<Row> rows = result.getRows();
        if (!columnIterator.isComplete() && (rows.size() > 0) && (rows.get(0).getPrimaryKey().equals(columnIterator.getPrimaryKey()))) {
            columnIterator.setColumnIterator(Arrays.asList(rows.get(0).getColumns()).iterator());
            idx = 1;
        }

        List<Pair<Row, Boolean>> rowsWithStatus = new ArrayList<Pair<Row, Boolean>>();
        for (int i = idx; i < rows.size(); i++) {
            if ((i == (rows.size() - 1)) && (rows.get(i).getPrimaryKey().equals(result.getNextStartPrimaryKey()))) {
                rowsWithStatus.add(new Pair<Row, Boolean>(rows.get(i), false));
            } else {
                rowsWithStatus.add(new Pair<Row, Boolean>(rows.get(i), true));
                rowsRead++;
            }
        }
        rowIterator = rowsWithStatus.iterator();

        nextStartPrimaryKey = result.getNextStartPrimaryKey();
        nextToken = result.getNextToken();
        if (!columnIterator.isComplete()) {
            if (!columnIterator.getPrimaryKey().equals(nextStartPrimaryKey)) {
                columnIterator.setComplete(true);
                rowsRead++;
            }
        }
    }

    @Override
    public boolean hasNext() {
        columnIterator.close();
        while ((nextStartPrimaryKey != null) && !rowIterator.hasNext() && (rowsRead < totalCount)) {
            fetchData();
        }
        if (rowIterator.hasNext()) {
            return true;
        }
        return false;
    }

    @Override
    public WideColumnIterator next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Pair<Row, Boolean> rowWithStatus = rowIterator.next();
        Row row = rowWithStatus.getFirst();
        boolean complete = rowWithStatus.getSecond();
        columnIterator = new RowRangeColumnIteratorImpl(this, row.getPrimaryKey(), Arrays.asList(row.getColumns()).iterator(), complete);
        return new WideColumnIterator(row.getPrimaryKey(), columnIterator);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
