package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchWriteRowResponse extends Response {
    /**
     * The result of a single row write in the BatchWriteRow batch operation.
     * If isSucceed is true, it means the write operation for this row succeeded.
     * If isSucceed is false, it means the write operation for this row failed, and the failure error information can be obtained through getError.
     */
    public static class RowResult {
        private boolean isSucceed = false;
        private String tableName;
        private Error error;
        private ConsumedCapacity consumedCapacity;
        private int index;
        private Row row;

        /**
         * internal use
         */
        public RowResult(String tableName, Row row, Error error, int index) {
            this.tableName = tableName;
            this.isSucceed = false;
            this.error = error;
            this.index = index;
            this.row = row;
        }

        /**
         * internal use
         */
        public RowResult(String tableName, Row row, ConsumedCapacity consumedCapacity, int index) {
            this.tableName = tableName;
            this.isSucceed = true;
            this.consumedCapacity = consumedCapacity;
            this.index = index;
            this.row = row;
        }

        /**
         * Determines whether the row query was executed successfully.
         * <p>{@link #consumedCapacity} is only valid when the execution is successful.</p>
         * <p>{@link #error} is only valid when the execution is not successful.</p>
         *
         * @return Returns true if the execution was successful, otherwise returns false.
         */
        public boolean isSucceed() {
            return isSucceed;
        }

        /**
         * Get the name of the table where this row is located.
         * <p>If the query for this row fails, you can retrieve the query parameters through {@link BatchGetRowRequest#getPrimaryKey(String, int)} using the table name and index to retry.</p>
         *
         * @return The name of the table
         */
        public String getTableName() {
            return tableName;
        }

        /**
         * If the query execution of this row fails, the specific error message is returned.
         *
         * @return If the execution fails, the error message is returned; otherwise, null is returned.
         */
        public Error getError() {
            return error;
        }

        /**
         * If the query of this row succeeds, the consumed capacity unit is returned.
         *
         * @return If the operation succeeds, the consumed capacity unit is returned; otherwise, null is returned.
         */
        public ConsumedCapacity getConsumedCapacity() {
            return consumedCapacity;
        }

        /**
         * Get the index position of this row in the multi-row query parameters of {@link BatchGetRowRequest}.
         * <p>If the query for this row fails, the query parameters can be retrieved through {@link BatchGetRowRequest#getPrimaryKey(String, int)} using the table name and index for a retry.</p>
         *
         * @return Index position
         */
        public int getIndex() {
            return index;
        }

        /**
         * Get the returned row data
         * @return If there is returned row data, return the row data; otherwise, return null
         */
        public Row getRow() {
            return row;
        }
    }

    private Map<String, List<RowResult>> tableToRowStatus;

    /**
     * internal use
     * @param meta
     */
    public BatchWriteRowResponse(Response meta) {
        super(meta);
        this.tableToRowStatus = new HashMap<String, List<RowResult>>();
    }

    /**
     * internal use
     * @param status
     */
    public void addRowResult(RowResult status) {
        String tableName = status.getTableName();

        List<RowResult> statuses = tableToRowStatus.get(tableName);
        if (statuses == null) {
            statuses = new ArrayList<RowResult>();
            tableToRowStatus.put(tableName, statuses);
        }
        statuses.add(status);
    }

    /**
     * Get the return results of all write operations on a table.
     *
     * @return The return result of the write operation, or null if the table does not exist.
     */
    public List<RowResult> getRowStatus(String tableName) {
        return tableToRowStatus.get(tableName);
    }

    /**
     * Get the return results of all table write operations.
     *
     * @return The return results of all table write operations.
     */
    public Map<String, List<RowResult>> getRowStatus() {
        return tableToRowStatus;
    }

    /**
     * Get all rows that failed to execute the PutRow operation.
     *
     * @return If there are rows that failed to execute, return all such rows; otherwise, return an empty list.
     */
    public List<RowResult> getFailedRows() {
        List<RowResult> result = new ArrayList<RowResult>();
        getResult(null, result);
        return result;
    }

    /**
     * Get all rows where operations are executed successfully.
     *
     * @return If there are rows executed successfully, return all rows; otherwise, return an empty list.
     */
    public List<RowResult> getSucceedRows() {
        List<RowResult> result = new ArrayList<RowResult>();
        getResult(result, null);
        return result;
    }

    /**
     * Get all rows that have been executed successfully and all rows that have failed execution.
     *
     * @param succeedRows All rows executed successfully
     * @param failedRows  All rows that failed execution
     */
    public void getResult(List<RowResult> succeedRows, List<RowResult> failedRows) {
        for (Map.Entry<String, List<RowResult>> entry : tableToRowStatus.entrySet()) {
            for (RowResult rs : entry.getValue()) {
                if (rs.isSucceed) {
                    if (succeedRows != null) {
                        succeedRows.add(rs);
                    }
                } else {
                    if (failedRows != null) {
                        failedRows.add(rs);
                    }
                }
            }
        }
    }

    /**
     * Whether all row modification operations have been executed successfully.
     *
     * @return If all row modification operations have been executed successfully, return true; otherwise, return false.
     */
    public boolean isAllSucceed() {
        return getFailedRows().isEmpty();
    }
}
