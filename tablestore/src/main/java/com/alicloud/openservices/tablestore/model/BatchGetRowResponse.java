/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 */

package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class BatchGetRowResponse extends Response implements Jsonizable {
    /**
     * The result of a single row query in the BatchGetRow batch operation.
     * If isSucceed is true, it means the row query operation was successful, and you can obtain the result of the single row query through getRow.
     * If isSucceed is false, it means the row query operation failed, and you can obtain the error information for the failure through getError.
     */
    public static class RowResult {
        private boolean isSucceed = false;
        private String tableName;
        private Error error;
        private Row row;
        private ConsumedCapacity consumedCapacity;
        private int index;
        private byte[] nextToken;

        public RowResult(String tableName, Error error, int index) {
            this.isSucceed = false;
            this.tableName = tableName;
            this.error = error;
            this.index = index;
        }
        
        public RowResult(String tableName, Row row, ConsumedCapacity consumedCapacity, int index) {
            this(tableName, row, consumedCapacity, index, null);
        }

        public RowResult(String tableName, Row row, ConsumedCapacity consumedCapacity, int index, byte[] nextToken) {
            this.isSucceed = true;
            this.tableName = tableName;
            this.row = row;
            this.consumedCapacity = consumedCapacity;
            this.index = index;
            this.nextToken = nextToken;
        }
        
        public boolean isSucceed() {
            return isSucceed;
        }

        public String getTableName() {
            return tableName;
        }

        public Error getError() {
            return error;
        }
        
        public Row getRow() {
            return row;
        }
        
        public ConsumedCapacity getConsumedCapacity() {
            return consumedCapacity;
        }

        public int getIndex() {
            return this.index;
        }

        public byte[] getNextToken() {
            return nextToken;
        }

        public boolean hasNextToken() {
            return (nextToken != null) && (nextToken.length > 0);
        }
    }

    /**
     * The return result of batch queries.
     */
    private Map<String, List<RowResult>> tableToRowsResult;

    /**
     * internal use
     */
    public BatchGetRowResponse(Response meta) {
        super(meta);
        this.tableToRowsResult = new HashMap<String, List<RowResult>>();
    }

    /**
     * internal use
     */
    public void addResult(RowResult result) {
        String tableName = result.getTableName();
        List<RowResult> tableRowResult = tableToRowsResult.get(tableName);
        if (tableRowResult == null) {
            tableRowResult = new ArrayList<RowResult>();
            tableToRowsResult.put(tableName, tableRowResult);
        }
        tableRowResult.add(result);
    }
    
    /**
     * Get the query results of all rows in a table.
     * @param tableName The name of the table
     * @return The query results of all rows under the table, if the table does not exist, return null.
     */
    public List<RowResult> getBatchGetRowResult(String tableName) {
        return tableToRowsResult.get(tableName);
    }

    /**
     * Returns the query results of all rows in all tables.
     * @return The result returned by this BatchGetRow operation.
     */
    public Map<String, List<RowResult>> getTableToRowsResult() {
        return tableToRowsResult;
    }

    /**
     * Get all rows that failed to execute the query operation.
     *
     * @return If there are rows that failed to execute, return all such rows; otherwise, return an empty list.
     */
    public List<RowResult> getFailedRows() {
        List<RowResult> result = new ArrayList<RowResult>();
        getResult(null, result);
        return result;
    }

    /**
     * Get all rows that the query operation executed successfully.
     *
     * @return If there are successfully executed rows, return all rows, otherwise return an empty list.
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
     * @param failedRows All rows that failed execution
     */
    public void getResult(List<RowResult> succeedRows, List<RowResult> failedRows) {
        for (Map.Entry<String, List<RowResult>> entry : tableToRowsResult.entrySet()) {
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
     * Whether all row query operations have been executed successfully.
     *
     * @return Returns true if all row query operations have been executed successfully, otherwise returns false.
     */
    public boolean isAllSucceed() {
        return getFailedRows().isEmpty();
    }

    @Override
    public String jsonize() {
        StringBuilder sb = new StringBuilder();
        jsonize(sb, "\n  ");
        return sb.toString();
    }

    @Override
    public void jsonize(StringBuilder sb, String newline) {
    	//Map<String, List<RowResult>> tableToRowsResult;
    	for (Map.Entry<String, List<RowResult>> tableResult : this.tableToRowsResult.entrySet()) {
    		for (RowResult result : tableResult.getValue()) {
    			sb.append("{\"TableName\": ");
    			sb.append(result.getTableName() + ", \"ConsumedCapacity\": ");
    	        result.getConsumedCapacity().jsonize(sb, newline + "  ");
    	        if (result.getRow() != null) {
    	        	sb.append(", \"Row\": " + result.getRow().toString());
    	        }
    		}
    	}
        sb.append("}");
    }
}
