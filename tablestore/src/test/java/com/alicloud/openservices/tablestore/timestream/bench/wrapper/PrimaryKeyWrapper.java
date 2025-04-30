package com.alicloud.openservices.tablestore.timestream.bench.wrapper;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;

import java.util.ArrayList;
import java.util.List;

public class PrimaryKeyWrapper {
    private List<PrimaryKeyColumn> primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();

    public static PrimaryKeyWrapper instance() {
        return new PrimaryKeyWrapper();
    }

    public PrimaryKeyWrapper addPrimaryKey(String name, long value) {
        return addPrimaryKey(name, PrimaryKeyValue.fromLong(value));
    }

    public PrimaryKeyWrapper addPrimaryKey(String name, String value) {
        return addPrimaryKey(name, PrimaryKeyValue.fromString(value));
    }

    public PrimaryKeyWrapper addPrimaryKey(String name, PrimaryKeyValue value) {
        primaryKeyColumns.add(new PrimaryKeyColumn(name, value));
        return this;
    }

    public PrimaryKey get() {
        return new PrimaryKey(primaryKeyColumns);
    }
}
