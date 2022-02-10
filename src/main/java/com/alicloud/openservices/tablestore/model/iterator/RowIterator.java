package com.alicloud.openservices.tablestore.model.iterator;

import java.util.Iterator;

import com.alicloud.openservices.tablestore.model.Row;

public interface RowIterator extends Iterator<Row> {
    long getTotalCount();
}
