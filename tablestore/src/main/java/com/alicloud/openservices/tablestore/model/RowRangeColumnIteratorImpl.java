package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.ClientException;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class RowRangeColumnIteratorImpl extends AbstractColumnIteratorImpl {

    private WideRowIterator wideRowIterator;
    private PrimaryKey primaryKey;
    private Iterator<Column> columnIterator;
    private boolean complete = false;
    private boolean close = false;

    public RowRangeColumnIteratorImpl(WideRowIterator wideRowIterator, PrimaryKey primaryKey,
                                      Iterator<Column> columnIterator, boolean complete) {
        this.wideRowIterator = wideRowIterator;
        this.primaryKey = primaryKey;
        this.columnIterator = columnIterator;
        this.complete = complete;
    }

    private boolean doHasNext() {
        while (!complete && !columnIterator.hasNext()) {
            this.wideRowIterator.fetchData();
        }
        if (columnIterator.hasNext()) {
            return true;
        }
        return false;
    }

    private Column doNext() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return columnIterator.next();
    }

    @Override
    public boolean hasNext() {
        if (close) {
            throw new ClientException("The iterator has been closed.");
        }
        return doHasNext();
    }

    @Override
    public Column next() {
        if (close) {
            throw new ClientException("The iterator has been closed.");
        }
        return doNext();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void setColumnIterator(Iterator<Column> columnIterator) {
        this.columnIterator = columnIterator;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public void close() {
        close = true;
        while (doHasNext()) {
            doNext();
        }
    }
}
