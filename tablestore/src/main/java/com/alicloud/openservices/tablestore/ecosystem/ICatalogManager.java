package com.alicloud.openservices.tablestore.ecosystem;

public interface ICatalogManager {
    /**
     * @param tableName
     * should be thread safe
     * @return the table catalog with index meta info
     */
    TableCatalog getTableCatalog(String tableName);
}
