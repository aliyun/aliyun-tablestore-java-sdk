package com.alicloud.openservices.tablestore.timestream.bench.wrapper;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class PutRowWrapper {
    private AsyncClient client;
    private List<PrimaryKeyColumn> primaryKeyColumns = new ArrayList<PrimaryKeyColumn>();
    private RowPutChange rowChange;

    public PutRowWrapper(AsyncClient client, String tableName) {
        this.client = client;
        this.rowChange = new RowPutChange(tableName);
    }

    public PutRowWrapper addPrimaryKey(String name, long value) {
        return addPrimaryKey(name, PrimaryKeyValue.fromLong(value));
    }

    public PutRowWrapper addPrimaryKey(String name, String value) {
        return addPrimaryKey(name, PrimaryKeyValue.fromString(value));
    }

    public PutRowWrapper addPrimaryKey(String name, PrimaryKeyValue value) {
        primaryKeyColumns.add(new PrimaryKeyColumn(name, value));
        return this;
    }

    public PutRowWrapper addColumn(String name, long value) {
        return addColumn(name, ColumnValue.fromLong(value));
    }

    public PutRowWrapper addColumn(String name, String value) {
        return addColumn(name, ColumnValue.fromString(value));
    }

    public PutRowWrapper addColumn(String name, ColumnValue value) {
        rowChange.addColumn(name, value);
        return this;
    }

    public PutRowResponse commit() throws ExecutionException, InterruptedException {
        PutRowRequest request = new PutRowRequest();
        rowChange.setPrimaryKey(new PrimaryKey(primaryKeyColumns));
        request.setRowChange(rowChange);
        return client.putRow(request, null).get();
    }
}
