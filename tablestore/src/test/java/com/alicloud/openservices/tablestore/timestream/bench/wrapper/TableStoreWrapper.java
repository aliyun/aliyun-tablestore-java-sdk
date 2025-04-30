package com.alicloud.openservices.tablestore.timestream.bench.wrapper;

import com.alicloud.openservices.tablestore.AsyncClient;

import java.util.concurrent.TimeUnit;

public class TableStoreWrapper {
    private AsyncClient client;
    private String tableName;

    public TableStoreWrapper(AsyncClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    public static TableStoreWrapper instance(AsyncClient client, String tableName) {
        return new TableStoreWrapper(client, tableName);
    }

    public CreateTableWrapper createTable() {
        return new CreateTableWrapper(client, tableName);
    }

    public void createTableAfter() throws InterruptedException {
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
    }

    public DeleteTableWrapper deleteTable() {
        return new DeleteTableWrapper();
    }

    public PutRowWrapper putRow() {
        return new PutRowWrapper(client, tableName);
    }
}
