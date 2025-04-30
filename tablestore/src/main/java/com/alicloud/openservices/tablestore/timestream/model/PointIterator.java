package com.alicloud.openservices.tablestore.timestream.model;

import com.alicloud.openservices.tablestore.model.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

public class PointIterator implements Iterator<Point> {
    private TimestreamIdentifier meta;
    private Iterator<Row> rowIterator;

    public PointIterator(Iterator<Row> iterator,
                         TimestreamIdentifier meta) {
        this.rowIterator = iterator;
        this.meta = meta;
    }

    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    public Point next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        Row row = rowIterator.next();
        Point point = new Point(row);
        return point;
    }

    public void remove() {
        throw new RuntimeException("PointIterator do not support remove().");
    }
}
