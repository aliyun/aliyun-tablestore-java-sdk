package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BulkImportResponse extends Response{

    private String tableName;

    public void setTableName(String tableName){
        this.tableName = tableName;
    }
    /**
     * The result of a single row write operation in the BatchWriteRow batch operation.
     * If isSucceed is true, it means the write operation for this row succeeded.
     * If isSucceed is false, it means the write operation for this row failed, and the failure error information can be obtained through getError.
     */
    public static class RowResult {
        private boolean isSucceed = false;
        private Error error;
        private ConsumedCapacity consumedCapacity;
        private int index;

        /**
         * internal use
         */
        public RowResult(Error error, int index) {
            this.isSucceed = false;
            this.error = error;
            this.index = index;
        }

        /**
         * internal use
         */
        public RowResult(ConsumedCapacity consumedCapacity, int index) {
            this.isSucceed = true;
            this.consumedCapacity = consumedCapacity;
            this.index = index;
        }

        /**
         * Determines whether the row query was executed successfully.
         * <p>{@link #consumedCapacity} is only valid when the operation is successful.</p>
         * <p>{@link #error} is only valid when the operation is not successful.</p>
         *
         * @return Returns true if the operation was successful, otherwise returns false.
         */
        public boolean isSucceed() {
            return isSucceed;
        }

        /**
         * If the query execution of this row fails, it returns specific error information.
         *
         * @return If the execution fails, it returns the error information; otherwise, it returns null.
         */
        public Error getError() {
            return error;
        }

        /**
         * If the row query succeeds, return the consumed capacity unit.
         *
         * @return If the execution succeeds, return the consumed capacity unit; otherwise, return null.
         */
        public ConsumedCapacity getConsumedCapacity() {
            return consumedCapacity;
        }

        /**
         * Get the index position of this row in the multi-row query parameters of {@link BatchGetRowRequest}.
         * <p>If the row query fails, you can retry by obtaining the query parameters using the table name and index through {@link BatchGetRowRequest#getPrimaryKey(String, int)}.</p>
         *
         * @return Index position
         */
        public int getIndex() {
            return index;
        }
    }

    private List<BulkImportResponse.RowResult> rowResults = new ArrayList<BulkImportResponse.RowResult>();
    /**
     * internal use
     * @param meta
     */
    public BulkImportResponse(Response meta) {
        super(meta);
    }

    /**
     * internal use
     * @param rowResults
     */
    public void addRowResult(BulkImportResponse.RowResult rowResults) {
        this.rowResults.add(rowResults);
    }

    /**
     * Get the return results of all write operations on a specific table.
     *
     * @return The return result of the write operation, or null if the table does not exist.
     */
    public List<BulkImportResponse.RowResult> getRowResults(){
        return rowResults;
    }

    /**
     * Get all rows that failed to execute the PutRow operation.
     *
     * @return If there are rows that failed to execute, return all such rows; otherwise, return an empty list.
     */
    public List<BulkImportResponse.RowResult> getFailedRows() {
        List<BulkImportResponse.RowResult> result = new ArrayList<BulkImportResponse.RowResult>();
        getResult(null, result);
        return result;
    }

    /**
     * Get all rows that operations have been executed successfully.
     *
     * @return If there are rows executed successfully, return all rows, otherwise return an empty list.
     */
    public List<BulkImportResponse.RowResult> getSucceedRows() {
        List<BulkImportResponse.RowResult> result = new ArrayList<BulkImportResponse.RowResult>();
        getResult(result, null);
        return result;
    }

    /**
     * Get all rows that were successfully executed and all rows that failed to execute.
     *
     * @param succeedRows All rows that were successfully executed
     * @param failedRows  All rows that failed to execute
     */
    public void getResult(List<BulkImportResponse.RowResult> succeedRows, List<BulkImportResponse.RowResult> failedRows) {
        for (BulkImportResponse.RowResult rs : rowResults) {
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

    /**
     * Whether all row modification operations have been executed successfully.
     *
     * @return If all row modification operations have been executed successfully, return true; otherwise, return false.
     */
    public boolean isAllSucceed() {
        return getFailedRows().isEmpty();
    }
}