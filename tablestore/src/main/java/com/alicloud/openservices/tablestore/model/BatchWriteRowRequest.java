package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.OptionalValue;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchWriteRowRequest extends TxnRequest {

    private Map<String, List<RowChange>> rowChangesGroupByTable;

    private OptionalValue<Boolean> isAtomic = new OptionalValue<Boolean>("IsAtomic");

    public BatchWriteRowRequest() {
        rowChangesGroupByTable = new HashMap<String, List<RowChange>>();
    }

    public String getOperationName() {
        return OperationNames.OP_BATCH_WRITE_ROW;
    }

    /**
     * Add write operation parameters for a specific table.
     *
     * @param rowChange Parameters for a write operation. The operation type can be Put, Update, or Delete. If Txn is used, each BatchWriteRow is only allowed for a single table.
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
     * Returns the parameters for a single write operation based on the table name and index.
     * The multi-row results returned in BatchWriteRowResult allow partial success and partial failure. The returned results are organized by table, and the order of rows within the table corresponds one-to-one with the order in BatchWriteRowRequest.
     * If the user needs to retry some failed rows from BatchWriteRowResult, they can look up the request parameters from BatchWriteRowRequest using the table name of the failed row's table and its index in the returned result list.
     *
     * @param tableName the name of the table
     * @param index     the index of this row in the parameter list
     * @return parameters for a single write operation
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
     * Get the operation parameters for all tables.
     *
     * @return The operation parameters for all tables.
     */
    public Map<String, List<RowChange>> getRowChange() {
        return rowChangesGroupByTable;
    }

    /**
     * Based on the returned result of the request, extract the rows that failed to execute and reconstruct a new request.
     *
     * @param failedRows    Rows that failed during the write operation
     * @return A new request for retry
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
        if (this.hasSetTransactionId()) {
            request.setTransactionId(this.getTransactionId());
        }
        if (this.isAtomicSet()) {
            request.setAtomic(this.isAtomic());
        }
        return request;
    }

    public boolean isEmpty() {
        return rowChangesGroupByTable.isEmpty();
    }

    /**
     * Get the total number of rows included in this BatchWriteRow request.
     *
     * @return The total number of rows
     */
    public int getRowsCount() {
        int rowsCount = 0;
        for (Map.Entry<String, List<RowChange>> entry : rowChangesGroupByTable.entrySet()) {
            rowsCount += entry.getValue().size();
        }
        return rowsCount;
    }

    /**
     * Whether the batch atomic write option is set.
     *
     * @return Whether the batch atomic write option is set
     */
    public boolean isAtomicSet() {
        return isAtomic.isValueSet();
    }

    /**
     * Set whether to use batch atomic writes.
     * If batch atomic write is enabled, you must ensure that the primary keys for writes to the same table are identical; otherwise, the write will fail.
     *
     * @param atomic Whether to use batch atomic writes
     */
    public void setAtomic(boolean atomic) {
        isAtomic.setValue(atomic);
    }

    /**
     * Check if it is a batch atomic write.
     *
     * @return whether it is a batch atomic write
     */
    public boolean isAtomic() {
        if (!isAtomic.isValueSet()) {
            throw new IllegalStateException("The value of isAtomic is not set.");
        }
        return isAtomic.getValue();
    }
}
