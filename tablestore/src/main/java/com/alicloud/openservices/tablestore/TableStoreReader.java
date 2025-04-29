package com.alicloud.openservices.tablestore;

import java.util.List;
import java.util.concurrent.Future;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowQueryCriteria;
import com.alicloud.openservices.tablestore.reader.PrimaryKeyWithTable;
import com.alicloud.openservices.tablestore.reader.ReaderResult;
import com.alicloud.openservices.tablestore.reader.RowReadResult;

public interface TableStoreReader {

    void addPrimaryKey(String tableName, PrimaryKey primaryKey);

    Future<ReaderResult> addPrimaryKeyWithFuture(String tableName, PrimaryKey primaryKey);

    void addPrimaryKeys(String tableName, List<PrimaryKey> primaryKeys);

    Future<ReaderResult> addPrimaryKeysWithFuture(String tableName, List<PrimaryKey> primaryKeys);

    void setRowQueryCriteria(RowQueryCriteria rowQueryCriteria);

    void send();

    void flush();

    void close();

    void setCallback(TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback);
}
