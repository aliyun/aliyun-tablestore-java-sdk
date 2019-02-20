package com.alicloud.openservices.tablestore.core.utils;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.WriterConfig;

import java.util.Map;

public class ParamChecker {
    /**
     * It does following checkings:
     * - primary key schema is matched with which defined in table meta.
     * - primary key column's value size is less than {@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxPKColumnSize}
     * - attribute column's value size is less than {@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxAttrColumnSize}
     * - the count of attribute columns is less than {@link com.alicloud.openservices.tablestore.writer.WriterConfig#maxColumnsCount} or zero.
     * - the name of attribute columns not duplicated with primary key column's name
     * - the total row size
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


        Map<String, PrimaryKeySchema> pkDefinedInMeta = tableMeta.getPrimaryKeySchemaMap();
        Map<String, PrimaryKeyColumn> pkInRow = rowChange.getPrimaryKey().getPrimaryKeyColumnsMap();
        if (pkDefinedInMeta.size() != pkInRow.size()) {
            throw new ClientException("The primary key schema is not match which defined in table meta.");
        }

        for (Map.Entry<String, PrimaryKeySchema> entry : pkDefinedInMeta.entrySet()) {
            PrimaryKeyValue value = pkInRow.get(entry.getKey()).getValue();

            // schema checking
            if (value == null) {
                throw new ClientException("Can't find primary key column '" + entry.getKey() + "' in row.");
            }

            if (value.isPlaceHolderForAutoIncr()) {
                if (entry.getValue().getOption() != PrimaryKeyOption.AUTO_INCREMENT) {
                    throw new ClientException("The type of primary key column '" + entry.getKey() + "' should not be AUTO_INCREMENT.");
                }
            } else if (value.getType() != entry.getValue().getType()) {
                throw new ClientException("The type of primary key column '" + entry.getKey() + "' is " + value.getType() +
                        ", but it's defined as " + entry.getValue().getType() + " in table meta.");
            }

            // value size checking
            if (value.getDataSize() > config.getMaxPKColumnSize()) {
                throw new ClientException("The size of primary key column '" + entry.getKey() + "' has exceeded the max length:" + config.getMaxPKColumnSize() + ".");
            }
        }

        int columnsCount = 0;
        if (rowChange instanceof RowPutChange) {
            RowPutChange rowPut = (RowPutChange) rowChange;
            columnsCount = rowPut.getColumnsToPut().size();
            for (Column column : rowPut.getColumnsToPut()) {
                checkColumn(pkDefinedInMeta, column, config);
            }
        } else if (rowChange instanceof RowUpdateChange) {
            RowUpdateChange rowUpdate = (RowUpdateChange) rowChange;
            columnsCount = rowUpdate.getColumnsToUpdate().size();
            for (Pair<Column, RowUpdateChange.Type> pair : rowUpdate.getColumnsToUpdate()) {
                checkColumn(pkDefinedInMeta, pair.first, config);
            }
        }

        if (columnsCount > config.getMaxColumnsCount()) {
            throw new ClientException("The count of attribute columns exceeds the maximum: " + config.getMaxColumnsCount() + ".");
        }
    }

    public static void checkColumn(Map<String, PrimaryKeySchema> pkDefinedInMeta, Column column, WriterConfig config) {
        if (pkDefinedInMeta.containsKey(column.getName())) {
            throw new ClientException("The attribute column's name duplicate with primary key column, which is '" + column.getName() + "'.");
        }

        ColumnValue value = column.getValue();
        if (value != null && value.getDataSize() > config.getMaxAttrColumnSize()) {
            throw new ClientException("The size of attribute column '" + column.getName() + "' has exceeded the max length: " + config.getMaxAttrColumnSize() + ".");
        }
    }
}
