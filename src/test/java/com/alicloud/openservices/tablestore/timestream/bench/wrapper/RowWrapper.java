package com.alicloud.openservices.tablestore.timestream.bench.wrapper;

import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.List;

public class RowWrapper {
    private PrimaryKeyWrapper primaryKeyWrapper = new PrimaryKeyWrapper();
    private List<Column> columns = new ArrayList<Column>();

    public static RowWrapper instance() {
        return new RowWrapper();
    }

    public RowWrapper addPrimaryKey(String name, long value) {
        primaryKeyWrapper.addPrimaryKey(name, value);
        return this;
    }

    public RowWrapper addPrimaryKey(String name, String value) {
        primaryKeyWrapper.addPrimaryKey(name, value);
        return this;
    }

    public RowWrapper addPrimaryKey(String name, PrimaryKeyValue value) {
        primaryKeyWrapper.addPrimaryKey(name, value);
        return this;
    }

    public RowWrapper addColumn(String name, long value) {
        return addColumn(name, ColumnValue.fromLong(value));
    }

    public RowWrapper addColumn(String name, String value) {
        return addColumn(name, ColumnValue.fromString(value));
    }

    public RowWrapper addColumn(String name, ColumnValue value) {
        columns.add(new Column(name, value, System.currentTimeMillis()));
        return this;
    }

    public Row get() {
        return new Row(primaryKeyWrapper.get(), columns);
    }
}
