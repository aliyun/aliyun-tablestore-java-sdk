package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ErrorCode;

import java.util.concurrent.ConcurrentHashMap;

public class CatalogManager implements ICatalogManager
{
    ConcurrentHashMap<String, TableCatalog> map;
    private SyncClient client;

    public CatalogManager(SyncClient client) {
        this.client = client;
        map = new ConcurrentHashMap<String, TableCatalog>();
        // todo start a backend thread to refresh the table catalog
    }

    @Override
    public TableCatalog getTableCatalog(String tableName) {
        TableCatalog catalog = map.get(tableName);
        if (catalog == null) {
            catalog = new TableCatalog(tableName);
            try {
                catalog.buildCatalog(client);
            } catch (TableStoreException e) {
                if (e.getErrorCode().equals(ErrorCode.OBJECT_NOT_EXIST)) {
                    return null;
                } else {
                    throw e;
                }
            }
            map.putIfAbsent(tableName, catalog);
        }
        return catalog;
    }
}
