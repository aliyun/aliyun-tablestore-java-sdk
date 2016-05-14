package com.aliyun.openservices.ots.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.openservices.ots.OTS;
import com.aliyun.openservices.ots.model.*;

public class OTSHelper {

    private static final Logger LOG = LoggerFactory.getLogger(OTSHelper.class);

    public static void deleteAllTable(OTS ots) throws Exception {
        LOG.info("Begin deleteAllTable");
        ListTableResult r = ots.listTable();

        for (String name : r.getTableNames()) {
            LOG.info("delete : " + name);
            deleteTable(ots, name);
        }
        LOG.info("End deleteAllTable");
    }

    public static TableMeta getTableMeta(String tableName, Map<String, PrimaryKeyType> pk) {
        TableMeta meta = new TableMeta(tableName);
        for (Map.Entry<String, PrimaryKeyType> entry : pk.entrySet()) {
            meta.addPrimaryKeyColumn(entry.getKey(), entry.getValue());
        }
        return meta;
    }

    public static void createTable(
            OTS ots,
            String tableName,
            Map<String, PrimaryKeyType> pk)  {
        CreateTableRequest request = new CreateTableRequest(getTableMeta(tableName, pk));
        request.setReservedThroughput(new CapacityUnit(5, 4));
        ots.createTable(request);
    }

    public static void createTable(
            OTS ots,
            TableMeta meta) {
        CreateTableRequest request = new CreateTableRequest();
        request.setTableMeta(meta);
        request.setReservedThroughput(new CapacityUnit(5, 4));
        ots.createTable(request);
    }

    public static void deleteTable(OTS ots, String tableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest(
                tableName);
        ots.deleteTable(deleteTableRequest);
    }

    public static List<String> listTable(OTS ots) {
        ListTableResult r = ots.listTable();
        return r.getTableNames();
    }

    public static DescribeTableResult describeTable(OTS ots, String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest(
                tableName);
        DescribeTableResult result = ots.describeTable(describeTableRequest);
        if (!result.getTableMeta().getTableName().equals(tableName)) {
            throw new RuntimeException("Wrong result.");
        }
        return result;
    }

    public static UpdateTableResult updateTable(OTS ots, String tableName,
                                                ReservedThroughputChange reservedThroughputChange) {
        UpdateTableRequest updateTableRequest = new UpdateTableRequest(
                tableName);
        if (reservedThroughputChange != null) {
            updateTableRequest
                    .setReservedThroughputChange(reservedThroughputChange);
        }
        return ots.updateTable(updateTableRequest);
    }

    public static PutRowResult putRow(OTS ots, String tableName, RowPrimaryKey pk,
                                      Map<String, ColumnValue> columns) {
        RowPutChange rowChange = new RowPutChange(tableName);
        rowChange.setPrimaryKey(pk);
        for (Map.Entry<String, ColumnValue> col : columns.entrySet()) {
            rowChange.addAttributeColumn(col.getKey(), col.getValue());
        }
        return putRow(ots, rowChange);
    }

    public static PutRowResult putRow(OTS ots, RowPutChange rowChange) {
        PutRowRequest putRowRequest = new PutRowRequest(rowChange);
        return ots.putRow(putRowRequest);
    }

    public static GetRowResult getRow(OTS ots, String tableName, RowPrimaryKey pk) {
        GetRowRequest request = new GetRowRequest();
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
        request.setRowQueryCriteria(criteria);
        return ots.getRow(request);
    }

    public static GetRowResult getRow(OTS ots,
                                      SingleRowQueryCriteria rowQueryCriteria) {
        GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
        GetRowResult result = ots.getRow(getRowRequest);
        return result;
    }

    public static UpdateRowResult updateRow(OTS ots, String tableName,
                                            RowPrimaryKey pk, Map<String, ColumnValue> puts,
                                            List<String> deleteColumn) {
        RowUpdateChange rowChange = new RowUpdateChange(tableName);
        rowChange.setPrimaryKey(pk);
        if (puts != null) {
            for (Map.Entry<String, ColumnValue> c : puts.entrySet()) {
                rowChange.addAttributeColumn(c.getKey(), c.getValue());
            }
        }
        if (deleteColumn != null) {
            for (String s : deleteColumn) {
                rowChange.deleteAttributeColumn(s);
            }
        }
        return updateRow(ots, rowChange);
    }

    public static UpdateRowResult updateRow(OTS ots, RowUpdateChange rowChange) {
        UpdateRowRequest updateRowRequest = new UpdateRowRequest(rowChange);
        return ots.updateRow(updateRowRequest);
    }

    public static DeleteRowResult deleteRow(OTS ots, String tableName,
                                            RowPrimaryKey pk) {
        RowDeleteChange rowChange = new RowDeleteChange(tableName);
        rowChange.setPrimaryKey(pk);
        return deleteRow(ots, rowChange);
    }

    public static DeleteRowResult deleteRow(OTS ots, RowDeleteChange rowChange) {
        DeleteRowRequest deleteRowRequest = new DeleteRowRequest(rowChange);
        return ots.deleteRow(deleteRowRequest);
    }

    public static BatchGetRowResult batchGetRow(
            OTS ots,
            List<MultiRowQueryCriteria> criterias) {
        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        for (MultiRowQueryCriteria criteria : criterias) {
            batchGetRowRequest.addMultiRowQueryCriteria(criteria);
        }
        return ots.batchGetRow(batchGetRowRequest);
    }

    public static BatchWriteRowResult batchWriteRow(
            OTS ots,
            List<RowPutChange> puts,
            List<RowUpdateChange> updates,
            List<RowDeleteChange> deletes) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        if (puts != null) {
            for (RowPutChange put : puts) {
                batchWriteRowRequest.addRowPutChange(put);
            }
        }
        if (updates != null) {
            for (RowUpdateChange update : updates) {
                batchWriteRowRequest.addRowUpdateChange(update);
            }
        }
        if (deletes != null) {
            for (RowDeleteChange delete : deletes) {
                batchWriteRowRequest.addRowDeleteChange(delete);
            }
        }
        return ots.batchWriteRow(batchWriteRowRequest);
    }

    public static GetRangeResult getRange(OTS ots,
                                          RangeRowQueryCriteria rangeRowQueryCriteria) {
        GetRangeRequest getRangeRequest = new GetRangeRequest(
                rangeRowQueryCriteria);
        return ots.getRange(getRangeRequest);
    }

    public static List<Row> getRangeForAll(OTS ots,
                                           RangeRowQueryCriteria rangeRowQueryCriteria) {
        List<Row> result = new ArrayList<Row>();
        GetRangeRequest getRangeRequest = new GetRangeRequest(
                rangeRowQueryCriteria);
        GetRangeResult r = ots.getRange(getRangeRequest);
        result.addAll(r.getRows());
        while (r.getNextStartPrimaryKey() != null) {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(r.getNextStartPrimaryKey());
            r = ots.getRange(getRangeRequest);
            result.addAll(r.getRows());
        }
        return result;
    }
}
