package com.alicloud.openservices.tablestore.model;

public class WideColumnIterator implements IRow {

    private PrimaryKey primaryKey;
    private AbstractColumnIteratorImpl columnIteratorImpl;

    public WideColumnIterator(PrimaryKey primaryKey, AbstractColumnIteratorImpl columnIteratorImpl) {
        this.primaryKey = primaryKey;
        this.columnIteratorImpl = columnIteratorImpl;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public int compareTo(IRow o) {
        return this.primaryKey.compareTo(o.getPrimaryKey());
    }

    public boolean hasNextColumn() {
        return columnIteratorImpl.hasNext();
    }

    public Column nextColumn() {
        return columnIteratorImpl.next();
    }
}
