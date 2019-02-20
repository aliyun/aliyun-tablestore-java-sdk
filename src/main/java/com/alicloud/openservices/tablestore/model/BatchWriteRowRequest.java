package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchWriteRowRequest extends TxnRequest {

    private Map<String, List<RowChange>> rowChangesGroupByTable;

    public BatchWriteRowRequest() {
        rowChangesGroupByTable = new HashMap<String, List<RowChange>>();
    }

    public String getOperationName() {
        return OperationNames.OP_BATCH_WRITE_ROW;
    }

    /**
     * 添加某个表的写操作参数。
     *
     * @param rowChange 一次写操作的参数，操作类型可以是Put、Update、Delete，如果使用Txn，则每次BatchWriteRow只能允许单张表。
     */
    public void addRowChange(RowChange rowChange) {
        Preconditions.checkNotNull(rowChange, "The rowPutChange should not be null.");
        String tableName = rowChange.getTableName();

        List<RowChange> rowChanges = rowChangesGroupByTable.get(tableName);
        if (rowChanges == null) {
            rowChanges = new ArrayList<RowChange>();
            rowChangesGroupByTable.put(tableName, rowChanges);
        }
        rowChanges.add(rowChange);
    }

    /**
     * 根据表名和索引返回一次写操作的参数。
     * BatchWriteRowResult中返回的多行结果允许部分成功部分失败，返回结果按表组织，且表内行的顺序与BatchWriteRowRequest中一一对应。
     * 若用户需要对BatchWriteRowResult中部分失败的行进行重试，则可以根据失败的行所在的表的表名以及在其在返回结果列表内的索引，从BatchWriteRowRequest中反查即可得到请求参数。
     *
     * @param tableName 表的名称
     * @param index     该行在参数列表中得索引
     * @return 一次写操作的参数
     */
    public RowChange getRowChange(String tableName, int index) {
        List<RowChange> rowChanges = rowChangesGroupByTable.get(tableName);
        if (rowChanges == null) {
            return null;
        }

        if (index >= rowChanges.size()) {
            return null;
        }
        return rowChanges.get(index);
    }

    /**
     * 获取所有表的操作参数。
     *
     * @return 所有表的操作参数。
     */
    public Map<String, List<RowChange>> getRowChange() {
        return rowChangesGroupByTable;
    }

    /**
     * 根据请求返回的结果，提取执行失败的行重新构造一次新的请求。
     *
     * @param failedRows    执行写操作失败的行
     * @return 新的用于重试的请求
     */
    public BatchWriteRowRequest createRequestForRetry(List<BatchWriteRowResponse.RowResult> failedRows) {
        Preconditions.checkArgument((failedRows != null) && !failedRows.isEmpty(), "failedRows can't be null or empty.");
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        for (BatchWriteRowResponse.RowResult rowResult : failedRows) {
            RowChange rowChange = getRowChange(rowResult.getTableName(), rowResult.getIndex());
            if (rowChange == null) {
                throw new IllegalArgumentException("Can not find item in table '" + rowResult.getTableName() + "' " +
                            "with index " + rowResult.getIndex());
            }
            request.addRowChange(rowChange);
        }
        return request;
    }

    public boolean isEmpty() {
        return rowChangesGroupByTable.isEmpty();
    }

    /**
     * 获取该BatchWriteRow请求中包含的总的行数。
     *
     * @return 总的行数
     */
    public int getRowsCount() {
        int rowsCount = 0;
        for (Map.Entry<String, List<RowChange>> entry : rowChangesGroupByTable.entrySet()) {
            rowsCount += entry.getValue().size();
        }
        return rowsCount;
    }
}
