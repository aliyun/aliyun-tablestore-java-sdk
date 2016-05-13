package com.aliyun.openservices.ots.model;

import com.aliyun.openservices.ots.internal.model.ResponseContentWithMeta;
import com.aliyun.openservices.ots.protocol.OtsProtocol2;
import com.aliyun.openservices.ots.protocol.OtsProtocol2.*;

import java.util.ArrayList;
import java.util.List;

public class OTSResultFactory {

    public static CreateTableResult createCreateTableResult(ResponseContentWithMeta response, CreateTableResponse createTableResponse) {
        CreateTableResult result = new CreateTableResult(response.getMeta());
        return result;
    }

    public static ListTableResult createListTableResult(
            ResponseContentWithMeta response,
            ListTableResponse listTableResponse) {
        ListTableResult result = new ListTableResult(response.getMeta());
        result.setTableNames(listTableResponse.getTableNamesList());
        return result;
    }

    public static DescribeTableResult createDescribeTableResult(
            ResponseContentWithMeta response,
            DescribeTableResponse describeTableResponse) {
        DescribeTableResult result = new DescribeTableResult(response.getMeta());
        result.setTableMeta(OTSProtocolHelper.parseTableMeta(describeTableResponse.getTableMeta()));
        result.setReservedThroughputDetails(OTSProtocolHelper.parseCapacityUnitDetails(describeTableResponse.getReservedThroughputDetails()));
        return result;
    }

    public static DeleteTableResult createDeleteTableResult(
            ResponseContentWithMeta response,
            DeleteTableResponse deleteTableResponse) {
        DeleteTableResult result = new DeleteTableResult(response.getMeta());
        return result;
    }

    public static UpdateTableResult createUpdateTableResult(
            ResponseContentWithMeta response,
            UpdateTableResponse updateTableResponse) {
        UpdateTableResult result = new UpdateTableResult(response.getMeta());
        ReservedThroughputDetails capacityUnitDetails = new ReservedThroughputDetails();
        com.aliyun.openservices.ots.protocol.OtsProtocol2.ReservedThroughputDetails details = updateTableResponse.getReservedThroughputDetails();
        capacityUnitDetails.setLastIncreaseTime(details.getLastIncreaseTime());
        if (details.hasLastDecreaseTime()) {
            capacityUnitDetails.setLastDecreaseTime(details.getLastDecreaseTime());
        }
        capacityUnitDetails.setNumberOfDecreasesToday(details.getNumberOfDecreasesToday());
        com.aliyun.openservices.ots.protocol.OtsProtocol2.CapacityUnit cu = details.getCapacityUnit();
        CapacityUnit capacityUnit = new CapacityUnit();
        if (cu.hasRead()) {
            capacityUnit.setReadCapacityUnit(cu.getRead());
        }
        if(cu.hasWrite()) {
            capacityUnit.setWriteCapacityUnit(cu.getWrite());
        }
        capacityUnitDetails.setCapacityUnit(capacityUnit);
        
        result.setReservedThroughputDetails(capacityUnitDetails);

        return result;
    }

    public static GetRowResult createGetRowResult(
            ResponseContentWithMeta response, GetRowResponse getRowResponse) {
        GetRowResult result = new GetRowResult(response.getMeta());
        result.setRow(OTSProtocolHelper.parseRow(getRowResponse.getRow()));
        result.setConsumedCapacity(OTSProtocolHelper.parseConsumedCapacity(getRowResponse.getConsumed()));
        return result;
    }

    public static PutRowResult createPutRowResult(
            ResponseContentWithMeta response, PutRowResponse putRowResponse) {
        PutRowResult result = new PutRowResult(response.getMeta());
        result.setConsumedCapacity(OTSProtocolHelper.parseConsumedCapacity(putRowResponse.getConsumed()));
        return result;
    }

    public static UpdateRowResult createUpdateRowResult(
            ResponseContentWithMeta response, UpdateRowResponse updateRowResponse) {
        UpdateRowResult result = new UpdateRowResult(response.getMeta());
        result.setConsumedCapacity(OTSProtocolHelper.parseConsumedCapacity(updateRowResponse.getConsumed()));
        return result;
    }

    public static DeleteRowResult createDeleteRowResult(
            ResponseContentWithMeta response, DeleteRowResponse deleteRowResponse) {
        DeleteRowResult result = new DeleteRowResult(response.getMeta());
        result.setConsumedCapacity(OTSProtocolHelper.parseConsumedCapacity(deleteRowResponse.getConsumed()));
        return result;
    }

    public static GetRangeResult createGetRangeResult(
            ResponseContentWithMeta response, GetRangeResponse getRangeResponse) {
        GetRangeResult result = new GetRangeResult(response.getMeta());
        result.setConsumedCapacity(OTSProtocolHelper.parseConsumedCapacity(getRangeResponse.getConsumed()));
        
        if (getRangeResponse.getNextStartPrimaryKeyCount() == 0) {
            // has no next primary key
            result.setNextStartPrimaryKey(null);
        } else {
            RowPrimaryKey nextStart = new RowPrimaryKey();
            result.setNextStartPrimaryKey(nextStart);
            for (com.aliyun.openservices.ots.protocol.OtsProtocol2.Column pbColumn : getRangeResponse.getNextStartPrimaryKeyList()) {
                nextStart.addPrimaryKeyColumn(pbColumn.getName(), OTSProtocolHelper.toPrimaryKeyValue(pbColumn.getValue()));
            }
        }
        
        List<Row> rows = new ArrayList<Row>();
        for (com.aliyun.openservices.ots.protocol.OtsProtocol2.Row row : getRangeResponse.getRowsList()) {
            rows.add(OTSProtocolHelper.parseRow(row));
        }
        result.setRows(rows);
        
        return result;
    }

    public static BatchGetRowResult createBatchGetRowResult(
            ResponseContentWithMeta response,
            BatchGetRowResponse batchGetRowResponse) {
        BatchGetRowResult result = new BatchGetRowResult(response.getMeta());
        
        for (TableInBatchGetRowResponse table : batchGetRowResponse.getTablesList()) {
            String tableName = table.getTableName();
            List<RowInBatchGetRowResponse> rowList = table.getRowsList();
            for (int i = 0; i < rowList.size(); i++) {
                result.addResult(OTSProtocolHelper.parseBatchGetRowStatus(tableName, rowList.get(i), i));
            }
        }
        return result;
    }

    public static BatchWriteRowResult createBatchWriteRowResult(
            ResponseContentWithMeta response,
            BatchWriteRowResponse batchWriteRowResponse) {
        BatchWriteRowResult result = new BatchWriteRowResult(response.getMeta());
        
        for (TableInBatchWriteRowResponse table : batchWriteRowResponse.getTablesList()) {
            String tableName = table.getTableName();

            List<RowInBatchWriteRowResponse> statuses = table.getPutRowsList();
            for (int i = 0; i < statuses.size(); i++) {
                result.addPutRowResult(OTSProtocolHelper.parseBatchWriteRowStatus(tableName, statuses.get(i), i));
            }

            statuses = table.getUpdateRowsList();
            for (int i = 0; i < statuses.size(); i++) {
                result.addUpdateRowResult(OTSProtocolHelper.parseBatchWriteRowStatus(tableName, statuses.get(i), i));
            }

            statuses = table.getDeleteRowsList();
            for (int i = 0; i < statuses.size(); i++) {
                result.addDeleteRowResult(OTSProtocolHelper.parseBatchWriteRowStatus(tableName, statuses.get(i), i));
            }
        }
        
        return result;
    }
}
