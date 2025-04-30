package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BulkImportRequest implements Request {
    private String tableName;
    private List<RowChange> rowChanges = new ArrayList<RowChange>();

    public BulkImportRequest(String tableName){
        Preconditions.checkArgument(
                tableName != null && !tableName.isEmpty(),
                "The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    public void setTableName(String tableName) {
        Preconditions.checkArgument(
                tableName != null && !tableName.isEmpty(),
                "The name of table should not be null or empty.");
        this.tableName = tableName;
    }

    public String getTableName(){
        return tableName;
    }

    public String getOperationName() {
        return OperationNames.OP_BULK_IMPORT;
    }

    public void addRowChange(RowChange rowChange) {
        Preconditions.checkNotNull(rowChange, "The rowPutChange should not be null.");
        this.rowChanges.add(rowChange);
    }

    public void addRowChanges(List<RowChange> rowChanges){
        Preconditions.checkNotNull(rowChanges, "The rowPutChanges should not be null.");
        for (RowChange rowChange : rowChanges){
            Preconditions.checkNotNull(rowChange, String.format("The rowPutChange should not be null."));
            this.rowChanges.add(rowChange);
        }
    }

    public List<RowChange> getRowChange(){
        return rowChanges;
    }

    public RowChange getRowChange(int index){
        return rowChanges.get(index);
    }

    /**
     * Generate a new BulkImportRequest based on the return result of a single BulkImportRequest.
     *
     * @return A new BulkImportRequest
     */
    public BulkImportRequest createRequestForRetry(List<BulkImportResponse.RowResult> failedRows) {
        Preconditions.checkArgument((failedRows != null) && !failedRows.isEmpty(), "failedRows can't be null or empty.");
        BulkImportRequest request = new BulkImportRequest(tableName);
        for (BulkImportResponse.RowResult rowResult : failedRows) {
            RowChange rowChange = getRowChange(rowResult.getIndex());
            if (rowChange == null) {
                throw new IllegalArgumentException("Can not find item in table '" + tableName + "' " +
                        "with index " + rowResult.getIndex());
            }
            request.addRowChange(rowChange);
        }
        return request;
    }
    /**
     * Determines whether the total number of rows included in the BulkImport request is empty.
     *
     * @return Whether the number of rows is empty
     */
    public boolean isEmpty() {
        return rowChanges.isEmpty();
    }

    /**
     * Get the total number of rows included in this BulkImport request.
     *
     * @return The total number of rows
     */
    public int getRowsCount() {
        return rowChanges.size();
    }

}