package com.aliyun.openservices.ots.utils;

import com.aliyun.openservices.ots.ClientException;
import com.aliyun.openservices.ots.internal.writer.WriterConfig;
import com.aliyun.openservices.ots.model.*;

import java.util.Map;

public class ParamChecker {
    /**
     * It does following checkings:
     *  - primary key schema is matched with which defined in table meta.
     *  - primary key column's value size is less than {@link com.aliyun.openservices.ots.internal.writer.WriterConfig#maxPKColumnSize}
     *  - attribute column's value size is less than {@link com.aliyun.openservices.ots.internal.writer.WriterConfig#maxAttrColumnSize}
     *  - the count of attribute columns is less than {@link WriterConfig#maxColumnsCount}
     *  - the name of attribute columns not duplicated with primary key column's name
     *  - the total row size
     *
     * @param tableMeta
     * @param rowChange
     * @param config
     * @throws ClientException
     */
    public static void checkRowChange(TableMeta tableMeta, RowChange rowChange, WriterConfig config)
        throws ClientException {
        // check table name
        if (!tableMeta.getTableName().equals(rowChange.getTableName())) {
            throw new ClientException("The row to write belongs to another table.");
        }

        // check row size
        if (rowChange.getDataSize() > config.getMaxBatchSize()) {
            throw new ClientException("The row size exceeds the max batch size: " + config.getMaxBatchSize() + ".");
        }


        Map<String, PrimaryKeyType> pkDefinedInMeta = tableMeta.getPrimaryKey();
        Map<String, PrimaryKeyValue> pkInRow = rowChange.getRowPrimaryKey().getPrimaryKey();
        if (pkDefinedInMeta.size() != pkInRow.size()) {
            throw new ClientException("The primary key schema is not match which defined in table meta.");
        }

        for (Map.Entry<String, PrimaryKeyType> entry : tableMeta.getPrimaryKey().entrySet()) {
            PrimaryKeyValue value = pkInRow.get(entry.getKey());

            // schema checking
            if (value == null) {
                throw new ClientException("Can't find primary key column '" + entry.getKey() + "' in row.");
            }

            if (value.getType() != entry.getValue()) {
                throw new ClientException("The type of primary key column '" + entry.getKey() + "' is " + value.getType() +
                        ", but it's defined as " + entry.getValue() + " in table meta.");
            }

            // value size checking
            if (value.getSize() > config.getMaxPKColumnSize()) {
                throw new ClientException("The size of primary key column '" + entry.getKey() + "' has exceeded the max length:" + config.getMaxPKColumnSize() + ".");
            }
        }

        Map<String, ColumnValue> attributes = null;
        if (rowChange instanceof RowPutChange) {
            RowPutChange rowPut = (RowPutChange)rowChange;
            attributes = rowPut.getAttributeColumns();
        } else if (rowChange instanceof RowUpdateChange) {
            RowUpdateChange rowUpdate = (RowUpdateChange)rowChange;
            attributes = rowUpdate.getAttributeColumns();
        }

        if (attributes != null) {
            if (attributes.size() > config.getMaxColumnsCount()) {
                throw new ClientException("The count of attribute columns exceeds the maximum: " + config.getMaxColumnsCount() + ".");
            }

            for (Map.Entry<String, ColumnValue> entry : attributes.entrySet()) {
                if (pkDefinedInMeta.containsKey(entry.getKey())) {
                    throw new ClientException("The attribute column's name duplicate with primary key column, which is '" + entry.getKey() + "'.");
                }

                ColumnValue value = entry.getValue();
                if (value != null && value.getSize() > config.getMaxAttrColumnSize()) {
                    throw new ClientException("The size of attribute column '" + entry.getKey() + "' has exceeded the max length: " + config.getMaxAttrColumnSize() + ".");
                }
            }
        }
    }
}
