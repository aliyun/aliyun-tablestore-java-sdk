package com.alicloud.openservices.tablestore.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse.RowResult;
import com.alicloud.openservices.tablestore.core.utils.Pair;

public class OTSHelper {

    private static final Logger LOG = LoggerFactory.getLogger(OTSHelper.class);

    public static TableMeta getTableMeta(String tableName, Map<String, PrimaryKeyType> pk) {
        TableMeta meta = new TableMeta(tableName);
        for (Map.Entry<String, PrimaryKeyType> entry : pk.entrySet()) {
            meta.addPrimaryKeyColumn(entry.getKey(), entry.getValue());
        }
        return meta;
    }

    public static void createTable(
            SyncClientInterface ots,
            String tableName,
            Map<String, PrimaryKeyType> pk)  {
        CreateTableRequest request = new CreateTableRequest(getTableMeta(tableName, pk), new TableOptions(-1, 1));
        request.setReservedThroughput(new ReservedThroughput(0, 0));
        ots.createTable(request);
    }
    
    public static void createTable(
            SyncClientInterface ots,
            String tableName,
            Map<String, PrimaryKeyType> pk,
            int readCU,
            int writeCU,
            int timeToLive,
            int maxVersions) {
        CreateTableRequest request = new CreateTableRequest(getTableMeta(tableName, pk), new TableOptions(timeToLive, maxVersions));
        request.setReservedThroughput(new ReservedThroughput(readCU, writeCU));
        ots.createTable(request);
    }

    public static void createTable(
            SyncClientInterface ots,
            TableMeta meta) {
        CreateTableRequest request = new CreateTableRequest(meta, getDefaultTableOptions());
        request.setReservedThroughput(new ReservedThroughput(0, 0));
        ots.createTable(request);
    }

    public static PutRowResponse putRow(SyncClientInterface ots, String tableName, PrimaryKey pk,
                                        Map<String, ColumnValue> columns) {
        RequestExtension extension = null;
        return putRow(ots, tableName, pk, columns, extension);
    }

    public static PutRowResponse putRow(SyncClientInterface ots, String tableName, PrimaryKey pk,
                                        Map<String, ColumnValue> columns, RequestExtension extension) {
        RowPutChange rowChange = new RowPutChange(tableName);
        rowChange.setPrimaryKey(pk);
        for (Map.Entry<String, ColumnValue> col : columns.entrySet()) {
            rowChange.addColumn(col.getKey(), col.getValue());
        }
        return putRow(ots, rowChange, extension);
    }

    public static PutRowResponse putRow(SyncClientInterface ots, String tableName, PrimaryKey pk,
                                        Map<String, ColumnValue> columns, RowExistenceExpectation rowExist,
                                        RequestExtension extension) {
        RowPutChange rowChange = new RowPutChange(tableName);
        rowChange.setPrimaryKey(pk);
        for (Map.Entry<String, ColumnValue> col : columns.entrySet()) {
            rowChange.addColumn(col.getKey(), col.getValue());
        }
        if (rowExist != null) {
            Condition condition = new Condition();
            condition.setRowExistenceExpectation(rowExist);
            rowChange.setCondition(condition);
        }
        return putRow(ots, rowChange, extension);
    }

    public static PutRowResponse putRow(SyncClientInterface ots, String tableName, PrimaryKey pk,
                                        Map<String, ColumnValue> columns, RowExistenceExpectation rowExist) {
        return putRow(ots, tableName, pk, columns, rowExist, null);
    }

    public static GetRowResponse getRow(SyncClientInterface ots, String tableName, PrimaryKey pk) {
        GetRowRequest request = new GetRowRequest();
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(tableName);
        criteria.setPrimaryKey(pk);
        criteria.setMaxVersions(1);
        request.setRowQueryCriteria(criteria);
        return ots.getRow(request);
    }



    public static void deleteAllTable(SyncClientInterface ots) throws Exception {
        LOG.info("Begin deleteAllTable");
        ListTableResponse r = ots.listTable();

        for (String name : r.getTableNames()) {
            LOG.info("delete : " + name);
            deleteTable(ots, name);
        }
        LOG.info("End deleteAllTable");
    }
    
    public static TableMeta getTableMeta(String tableName, List<PrimaryKeySchema> pk) {
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(pk);
        return meta;
    }

    public static void createTable(
            SyncClientInterface ots,
            String tableName,
            List<PrimaryKeySchema> pk) {

        createTable(ots, getTableMeta(tableName, pk), new CapacityUnit(0, 0), null);
    }

    public static void createTable(
            SyncClientInterface ots,
            String tableName,
            List<PrimaryKeySchema> scheme,
            int ttl,
            int maxVersions) {
        TableMeta meta = new TableMeta(tableName);
        meta.addPrimaryKeyColumns(scheme);

        TableOptions to = new TableOptions();
        to.setTimeToLive(ttl);
        to.setMaxVersions(maxVersions);
        createTable(ots, meta, new CapacityUnit(0, 0), to);
    }
    
    public static TableOptions getDefaultTableOptions() {
    	TableOptions tableOptions = new TableOptions();
        tableOptions.setTimeToLive(OTSTestConst.DEFAULT_TTL);
        tableOptions.setMaxVersions(OTSTestConst.DEFAULT_MAX_VERSION);
        tableOptions.setMaxTimeDeviation(Long.MAX_VALUE / 1000000);
        return tableOptions;
    }

    public static void createTable(
            SyncClientInterface ots,
            TableMeta meta,
            CapacityUnit capacityUnit,
            TableOptions tableOptions) {
        if (tableOptions == null) {
            tableOptions = getDefaultTableOptions();
        }
        CreateTableRequest createTableRequest = new CreateTableRequest(meta, tableOptions);
        createTableRequest.setReservedThroughput(new ReservedThroughput(
                capacityUnit));

        ots.createTable(createTableRequest);
    }

    public static void deleteTable(SyncClientInterface ots, String tableName) {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest(
                tableName);
        ots.deleteTable(deleteTableRequest);
    }

    public static List<String> listTable(SyncClientInterface ots) {
        ListTableResponse r = ots.listTable();
        return r.getTableNames();
    }

    public static DescribeTableResponse describeTable(SyncClientInterface ots, String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest(
                tableName);
        DescribeTableResponse result = ots.describeTable(describeTableRequest);
        if (!result.getTableMeta().getTableName().equals(tableName)) {
            throw new RuntimeException("Wrong result.");
        }
        return result;
    }

    public static UpdateTableResponse updateTable(SyncClientInterface ots, String tableName,
                                                  ReservedThroughput reservedThroughputForUpdate,
                                                  TableOptions tableOptionsForUpdate) {
        UpdateTableRequest updateTableRequest = new UpdateTableRequest(
                tableName);
        if (reservedThroughputForUpdate != null) {
            updateTableRequest
                    .setReservedThroughputForUpdate(reservedThroughputForUpdate);
        }
        if (tableOptionsForUpdate != null) {
            updateTableRequest.setTableOptionsForUpdate(tableOptionsForUpdate);
        }
        return ots.updateTable(updateTableRequest);
    }

    public static PutRowResponse putRow(SyncClientInterface ots, String tableName, PrimaryKey pk,
                                        List<Column> columns) {
        RowPutChange rowChange = new RowPutChange(tableName, pk);
        for (Column col : columns) {
            rowChange.addColumn(col);
        }

        return putRow(ots, rowChange);
    }

    public static PutRowResponse putRow(SyncClientInterface ots, RowPutChange rowChange) {
        return putRow(ots, rowChange, null);
    }

    public static PutRowResponse putRow(SyncClientInterface ots, RowPutChange rowChange, RequestExtension extension) {
        PutRowRequest putRowRequest = new PutRowRequest(rowChange);
        putRowRequest.setExtension(extension);
        return ots.putRow(putRowRequest);
    }

    public static GetRowResponse getRowForAll(SyncClientInterface ots, String tableName, PrimaryKey pk) {
        return getRow(ots, tableName, pk, null, Integer.MAX_VALUE);
    }
    
    public static GetRowResponse getRow(SyncClientInterface ots, String tableName, PrimaryKey pk, long ts) {
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                tableName, pk);
        rowQueryCriteria.setTimestamp(ts);
        return getRow(ots, rowQueryCriteria);
    }
    
    public static GetRowResponse getRow(SyncClientInterface ots, String tableName, PrimaryKey pk, TimeRange range, int maxVersion) {
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                tableName, pk);
        if (range != null) {
            rowQueryCriteria.setTimeRange(range);
        }
        if (maxVersion > 0) {
            rowQueryCriteria.setMaxVersions(maxVersion);
        }
        return getRow(ots, rowQueryCriteria);
    }

    public static GetRowResponse getRow(SyncClientInterface ots, String tableName, PrimaryKey pk, TimeRange range, int maxVersion, List<String> columnToGet) {
        SingleRowQueryCriteria rowQueryCriteria = new SingleRowQueryCriteria(
                tableName, pk);
        rowQueryCriteria.addColumnsToGet(columnToGet);
        if (range != null) {
            rowQueryCriteria.setTimeRange(range);
        }
        if (maxVersion > 0) {
            rowQueryCriteria.setMaxVersions(maxVersion);
        }
        return getRow(ots, rowQueryCriteria);
    }

    public static GetRowResponse getRow(SyncClientInterface ots,
                                        SingleRowQueryCriteria rowQueryCriteria) {
        return getRow(ots, rowQueryCriteria, null);
    }

    public static GetRowResponse getRow(SyncClientInterface ots,
                                        SingleRowQueryCriteria rowQueryCriteria,
                                        RequestExtension extension) {
        GetRowRequest getRowRequest = new GetRowRequest(rowQueryCriteria);
        getRowRequest.setExtension(extension);
        GetRowResponse result = ots.getRow(getRowRequest);
        if (result.getRow() != null && !result.getRow().getPrimaryKey().equals(rowQueryCriteria.getPrimaryKey())) {
            throw new RuntimeException("Wrong result.");
        }
        return result;
    }


    public static UpdateRowResponse updateRow(SyncClientInterface ots, String tableName,
                                              PrimaryKey pk, List<Column> puts, List<String> deleteColumn,
                                              List<Pair<String, Long>> deleteCell) {
        return updateRow(ots, tableName, pk, puts, deleteColumn, deleteCell, RowExistenceExpectation.IGNORE);
    }

    public static UpdateRowResponse updateRow(SyncClientInterface ots, String tableName,
                                              PrimaryKey pk, List<Column> puts, List<String> deleteColumn,
                                              List<Pair<String, Long>> deleteCell, RowExistenceExpectation rowExist) {
        RowUpdateChange rowChange = new RowUpdateChange(tableName, pk);
        if (puts != null) {
            for (Column c : puts) {
                rowChange.put(c);
            }
        }
        if (deleteColumn != null) {
            for (String s : deleteColumn) {
                rowChange.deleteColumns(s);
            }
        }
        if (deleteCell != null) {
            for (Pair<String, Long> s : deleteCell) {
                rowChange.deleteColumn(s.getFirst(), s.getSecond());
            }
        }
        if (rowExist != null) {
        	Condition condition = new Condition();
        	condition.setRowExistenceExpectation(rowExist);
        	rowChange.setCondition(condition);
        }
        return updateRow(ots, rowChange);
    }

    public static UpdateRowResponse updateRow(SyncClientInterface ots, RowUpdateChange rowChange) {
        UpdateRowRequest updateRowRequest = new UpdateRowRequest(rowChange);
        return ots.updateRow(updateRowRequest);
    }

    public static DeleteRowResponse deleteRow(SyncClientInterface ots, String tableName,
                                              PrimaryKey pk) {
        RowDeleteChange rowChange = new RowDeleteChange(tableName, pk);
        return deleteRow(ots, rowChange);
    }

    public static DeleteRowResponse deleteRow(SyncClientInterface ots, String tableName,
                                              PrimaryKey pk, RowExistenceExpectation rowExist) {
        RowDeleteChange rowChange = new RowDeleteChange(tableName, pk);
    	if (rowExist != null) {
        	Condition condition = new Condition();
    		condition.setRowExistenceExpectation(rowExist);
            rowChange.setCondition(condition);
    	}
        return deleteRow(ots, rowChange);
    }

    public static DeleteRowResponse deleteRow(SyncClientInterface ots, RowDeleteChange rowChange) {
        DeleteRowRequest deleteRowRequest = new DeleteRowRequest(rowChange);
        return ots.deleteRow(deleteRowRequest);
    }
    
    private static List<Row> getRows(SyncClientInterface ots, List<Pair<String, PrimaryKey>> pks) {
        List<MultiRowQueryCriteria> criterias = new ArrayList<MultiRowQueryCriteria>();
        for (Pair<String, PrimaryKey> p : pks) {
            MultiRowQueryCriteria c = new MultiRowQueryCriteria(p.getFirst());
            c.setMaxVersions(Integer.MAX_VALUE);
            criterias.add(c);
        }
        
        List<RowResult> rr = batchGetRow(ots, criterias).getSucceedRows();
        List<Row> rows = new ArrayList<Row>();
        for (RowResult r : rr) {
            rows.add(r.getRow());
        }
        return rows;
    }
    
    public static List<Row> batchGetRowForAllRow(
            SyncClientInterface ots,
            List<Pair<String, PrimaryKey>> pks) {
        List<Row> rows = new ArrayList<Row>();
        
        for (int i = 0; i < pks.size(); i += OTSRestrictedItemConst.BATCH_GET_ROW_COUNT_MAX) {
            if (i + OTSRestrictedItemConst.BATCH_GET_ROW_COUNT_MAX < pks.size()) {
                rows.addAll(getRows(ots, pks.subList(i, i + OTSRestrictedItemConst.BATCH_GET_ROW_COUNT_MAX)));
            } else {
                rows.addAll(getRows(ots, pks.subList(i, pks.size())));
            }
        }
        return rows;
    }

    public static BatchGetRowResponse batchGetRow(
            SyncClientInterface ots,
            List<MultiRowQueryCriteria> criterias) {
        BatchGetRowRequest batchGetRowRequest = new BatchGetRowRequest();
        for (MultiRowQueryCriteria criteria : criterias) {
            batchGetRowRequest.addMultiRowQueryCriteria(criteria);
        }
        return ots.batchGetRow(batchGetRowRequest);
    }
    
    public static List<RowResult> batchGetRowNoLimit(
            SyncClientInterface ots,
            List<MultiRowQueryCriteria> criterias) {
        List<RowResult> rr = new ArrayList<RowResult>();
        for (MultiRowQueryCriteria c : criterias) {
            MultiRowQueryCriteria cc = new MultiRowQueryCriteria(c.getTableName());
            LOG.info("Table: {}, count:{}", c.getTableName(), c.getRowKeys().size());
            cc.setMaxVersions(Integer.MAX_VALUE);
            for (PrimaryKey pk : c.getRowKeys()) {
                if (cc.getRowKeys().size() == OTSRestrictedItemConst.BATCH_GET_ROW_COUNT_MAX) {
                    List<MultiRowQueryCriteria> ccs = new ArrayList<MultiRowQueryCriteria>();
                    ccs.add(cc);
                    
                    BatchGetRowResponse r = batchGetRow(ots, ccs);
                    LOG.info("Fail size : " + r.getFailedRows().size());
                    LOG.info("Succ size : " + r.getSucceedRows().size());
                    if (!r.isAllSucceed()) {
                        for (RowResult row : r.getFailedRows()) {
                            LOG.error("Fail : {}, {}", row.getError().getCode(), row.getError().getMessage());
                            throw new RuntimeException(String.format("BatchGetRow fail. code :%s, msg:%s", row.getError().getCode(), row.getError().getMessage()));
                        }
                    }
                    
                    rr.addAll(r.getSucceedRows());
                    cc = new MultiRowQueryCriteria(c.getTableName());
                    cc.setMaxVersions(Integer.MAX_VALUE);
                    cc.addRow(pk);
                } else {
                    cc.addRow(pk);
                }
            }
            List<MultiRowQueryCriteria> ccs = new ArrayList<MultiRowQueryCriteria>();
            ccs.add(cc);
            BatchGetRowResponse r = batchGetRow(ots, ccs);
            LOG.info("Fail size : " + r.getFailedRows().size());
            LOG.info("Succ size : " + r.getSucceedRows().size());
            if (!r.isAllSucceed()) {
                for (RowResult row : r.getFailedRows()) {
                    LOG.error("Fail : {}, {}", row.getError().getCode(), row.getError().getMessage());
                    throw new RuntimeException(String.format("BatchGetRow fail. code :%s, msg:%s", row.getError().getCode(), row.getError().getMessage()));
                }
            }
            rr.addAll(r.getSucceedRows());
        }
        return rr;
    }

    public static BatchWriteRowResponse batchWriteRow(
            SyncClientInterface ots,
            List<RowPutChange> puts,
            List<RowUpdateChange> updates,
            List<RowDeleteChange> deletes,
            RequestExtension extension) {
        BatchWriteRowRequest batchWriteRowRequest = new BatchWriteRowRequest();
        batchWriteRowRequest.setExtension(extension);
        if (puts != null) {
            for (RowPutChange put : puts) {
                batchWriteRowRequest.addRowChange(put);
            }
        }
        if (updates != null) {
            for (RowUpdateChange update : updates) {
                batchWriteRowRequest.addRowChange(update);
            }
        }
        if (deletes != null) {
            for (RowDeleteChange delete : deletes) {
                batchWriteRowRequest.addRowChange(delete);
            }
        }
        return ots.batchWriteRow(batchWriteRowRequest);
    }

    public static BatchWriteRowResponse batchWriteRow(
            SyncClientInterface ots,
            List<RowPutChange> puts,
            List<RowUpdateChange> updates,
            List<RowDeleteChange> deletes) {
        return batchWriteRow(ots, puts, updates, deletes, null);
    }
    
    public static void batchWriteRowNoLimit(
            SyncClientInterface ots,
            List<RowPutChange> puts,
            List<RowUpdateChange> updates,
            List<RowDeleteChange> deletes) {
        int count = 0;
        if (puts != null)
        {
            List<RowPutChange> newPuts = new ArrayList<RowPutChange>();
            for (RowPutChange put : puts) {
                if (newPuts.size() == OTSRestrictedItemConst.BATCH_WRITE_ROW_COUNT_MAX) {
                    count += newPuts.size();
                    BatchWriteRowResponse r = batchWriteRow(ots, newPuts, null, null);
                    LOG.info("RequestID:{}", r.getRequestId());
                    List<BatchWriteRowResponse.RowResult> s = r.getFailedRows();
                    for (BatchWriteRowResponse.RowResult rr : s) {
                        LOG.error("batchWriteRow-put fail, {}, {}", rr.getError().getCode(), rr.getError().getMessage());
                        throw new RuntimeException(String.format("batchWriteRow-put fail, %s, %s", rr.getError().getCode(), rr.getError().getMessage()));
                    }
                    newPuts.clear();
                }
                newPuts.add(put);
            }
            if (!newPuts.isEmpty()) {
                count += newPuts.size();
                BatchWriteRowResponse r = batchWriteRow(ots, newPuts, null, null);
                LOG.info("RequestID:{}", r.getRequestId());
                List<BatchWriteRowResponse.RowResult> s = r.getFailedRows();
                for (BatchWriteRowResponse.RowResult rr : s) {
                    LOG.error("batchWriteRow-put fail, {}, {}", rr.getError().getCode(), rr.getError().getMessage());
                    throw new RuntimeException(String.format("batchWriteRow-put fail, %s, %s", rr.getError().getCode(), rr.getError().getMessage()));
                }
            }
        }
        if (updates != null)
        {
            List<RowUpdateChange> newUpdates = new ArrayList<RowUpdateChange>();
            for (RowUpdateChange update : updates) {
                if (newUpdates.size() == OTSRestrictedItemConst.BATCH_WRITE_ROW_COUNT_MAX) {
                    count += newUpdates.size();
                    BatchWriteRowResponse r = batchWriteRow(ots, null, newUpdates, null);
                    LOG.info("RequestID:{}", r.getRequestId());
                    List<BatchWriteRowResponse.RowResult> s = r.getFailedRows();
                    for (BatchWriteRowResponse.RowResult rr : s) {
                        LOG.error("batchWriteRow-update fail, {}, {}", rr.getError().getCode(), rr.getError().getMessage());
                        throw new RuntimeException(String.format("batchWriteRow-update fail, %s, %s", rr.getError().getCode(), rr.getError().getMessage()));
                    }
                    newUpdates.clear();
                }
                newUpdates.add(update);
            }
            
            if (!newUpdates.isEmpty()) {
                count += newUpdates.size();
                BatchWriteRowResponse r = batchWriteRow(ots, null, newUpdates, null);
                LOG.info("RequestID:{}", r.getRequestId());
                List<BatchWriteRowResponse.RowResult> s = r.getFailedRows();
                for (BatchWriteRowResponse.RowResult rr : s) {
                    LOG.error("batchWriteRow-update fail, {}, {}", rr.getError().getCode(), rr.getError().getMessage());
                    throw new RuntimeException(String.format("batchWriteRow-update fail, %s, %s", rr.getError().getCode(), rr.getError().getMessage()));
                }
            }
        }
        
        if (deletes != null)
        {
            List<RowDeleteChange> newDeletes = new ArrayList<RowDeleteChange>();
            for (RowDeleteChange delete : deletes) {
                if (newDeletes.size() == OTSRestrictedItemConst.BATCH_WRITE_ROW_COUNT_MAX) {
                    count += newDeletes.size();
                    BatchWriteRowResponse r = batchWriteRow(ots, null, null, newDeletes);
                    LOG.info("RequestID:{}", r.getRequestId());
                    List<BatchWriteRowResponse.RowResult> s = r.getFailedRows();
                    for (BatchWriteRowResponse.RowResult rr : s) {
                        LOG.error("batchWriteRow-delete fail, {}, {}", rr.getError().getCode(), rr.getError().getMessage());
                        throw new RuntimeException(String.format("batchWriteRow-delete fail, %s, %s", rr.getError().getCode(), rr.getError().getMessage()));
                    }
                    newDeletes.clear();
                }
                newDeletes.add(delete);
            }
            
            if (!newDeletes.isEmpty()) {
                count += newDeletes.size();
                BatchWriteRowResponse r = batchWriteRow(ots, null, null, newDeletes);
                LOG.info("RequestID:{}", r.getRequestId());
                List<BatchWriteRowResponse.RowResult> s = r.getFailedRows();
                for (BatchWriteRowResponse.RowResult rr : s) {
                    LOG.error("batchWriteRow-delete fail, {}, {}", rr.getError().getCode(), rr.getError().getMessage());
                    throw new RuntimeException(String.format("batchWriteRow-delete fail, %s, %s", rr.getError().getCode(), rr.getError().getMessage()));
                }
            }
        }
        LOG.info("BatchWriteRow Count : {}", count);
    }

    public static GetRangeResponse getRange(SyncClientInterface ots,
                                            RangeRowQueryCriteria rangeRowQueryCriteria) {
        return getRange(ots, rangeRowQueryCriteria, null);
    }

    public static GetRangeResponse getRange(SyncClientInterface ots,
                                            RangeRowQueryCriteria rangeRowQueryCriteria,
                                            RequestExtension extension) {
        GetRangeRequest getRangeRequest = new GetRangeRequest(
                rangeRowQueryCriteria);
        getRangeRequest.setExtension(extension);
        return ots.getRange(getRangeRequest);
    }

    public static List<Row> getRangeForAll(SyncClientInterface ots,
                                           RangeRowQueryCriteria rangeRowQueryCriteria) {
        List<Row> result = new ArrayList<Row>();
        GetRangeRequest getRangeRequest = new GetRangeRequest(
                rangeRowQueryCriteria);
        GetRangeResponse r = ots.getRange(getRangeRequest);
        result.addAll(r.getRows());
        while (r.getNextStartPrimaryKey() != null) {
            rangeRowQueryCriteria.setInclusiveStartPrimaryKey(r.getNextStartPrimaryKey());
            r = ots.getRange(getRangeRequest);
            result.addAll(r.getRows());
        }
        return result;
    }
}
