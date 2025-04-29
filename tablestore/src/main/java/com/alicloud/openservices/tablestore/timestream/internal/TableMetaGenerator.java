package com.alicloud.openservices.tablestore.timestream.internal;

import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.TableMeta;

public class TableMetaGenerator {
    // hashcode
    public static final String CN_PK0 = "h";

    // name
    public static final String CN_PK1 = "n";

    // tags
    public static final String CN_PK2 = "t";

    // timestamp
    public static final String CN_TAMESTAMP_NAME = "s";

    public static TableMeta getDataTableMeta(String tableName) {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(CN_PK0, PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn(CN_PK1, PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn(CN_PK2, PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn(CN_TAMESTAMP_NAME, PrimaryKeyType.INTEGER);
        return tableMeta;
    }

    public static TableMeta getMetaTableMeta(String tableName) {
        TableMeta tableMeta = new TableMeta(tableName);
        tableMeta.addPrimaryKeyColumn(CN_PK0, PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn(CN_PK1, PrimaryKeyType.STRING);
        tableMeta.addPrimaryKeyColumn(CN_PK2, PrimaryKeyType.STRING);
        return tableMeta;
    }
}
