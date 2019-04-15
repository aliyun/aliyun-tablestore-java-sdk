package com.alicloud.openservices.tablestore.timestream.bench.wrapper;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CreateTableWrapper {
    private AsyncClient client;
    private TableMeta tableMeta;
    private TableOptions tableOptions;

    public CreateTableWrapper(AsyncClient client, String tableName) {
        this.client = client;
        this.tableMeta = new TableMeta(tableName);
        this.tableOptions = new TableOptions();
    }

    public CreateTableWrapper addPrimaryKey(String name, PrimaryKeyType type) {
        tableMeta.addPrimaryKeyColumn(name, type);
        return this;
    }

    public CreateTableWrapper setMaxVersion(int version) {
        tableOptions.setMaxVersions(version);
        return this;
    }

    public CreateTableWrapper setTTL(int ttl, TimeUnit unit) {
        tableOptions.setTimeToLive((int)unit.toSeconds(ttl));
        return this;
    }

    public CreateTableResponse commit() throws ExecutionException, InterruptedException {
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        CreateTableResponse response = client.createTable(request, null).get();
        return response;
    }

    public void commitIgnore() throws ExecutionException, InterruptedException {
        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        try {
            client.createTable(request, null).get();
        } catch (TableStoreException e) {
            if (!e.getErrorCode().equals(ErrorCode.OTS_OBJECT_ALREADY_EXIST)) {
                throw e;
            }
        }
    }
}