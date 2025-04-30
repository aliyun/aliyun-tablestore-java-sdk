package com.alicloud.openservices.tablestore.reader;

import java.util.Map;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.alicloud.openservices.tablestore.model.TableMeta;

public class ReaderUtils {

    public static void checkTableMeta(TableMeta meta, PrimaryKey primaryKey) {
        if (meta.getPrimaryKeyList().size() != primaryKey.size()) {
            throw new ClientException("In table:" + meta.getTableName() + ", the size of primaryKey:" + primaryKey.size() + " is not equals to that of the table meta:" + meta.getPrimaryKeyList().size() + ".");
        }

        for (Map.Entry<String, PrimaryKeyColumn> entry : primaryKey.getPrimaryKeyColumnsMap().entrySet()) {
            if (!meta.getPrimaryKeyMap().containsKey(entry.getKey())) {
                throw new ClientException("In table:" + meta.getTableName() + ", table do not contains primaryKey:" + entry.getKey());
            }
            PrimaryKeyType typeInMeta = meta.getPrimaryKeyMap().get(entry.getKey());
            PrimaryKeyType typeInPrimaryKey = entry.getValue().getValue().getType();

            if (typeInMeta != typeInPrimaryKey) {
                throw new ClientException(
                        "In table:" + meta.getTableName() +
                                ", primaryKey name : " + entry.getKey() +
                                ": the type in meta [" + typeInMeta +
                                "] does not equals to that in primaryKey [" + typeInPrimaryKey + "]");
            }
        }
    }
}
