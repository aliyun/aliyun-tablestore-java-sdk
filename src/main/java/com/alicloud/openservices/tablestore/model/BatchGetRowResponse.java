/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alicloud.openservices.tablestore.core.utils.Jsonizable;

public class BatchGetRowResponse extends Response implements Jsonizable {
    /**
     * BatchGetRow批量操作中单行查询的结果。
     * 若isSucceed为true，则代表该行查询操作成功，可以通过getRow获取单行查询的结果。
     * 若isSucceed为false，则代表该行查询操作失败，可以通过getError获取失败的错误信息。
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
     * 批量查询的返回结果。
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
     * 获取某个表的所有行查询结果。
     * @param tableName 表的名称
     * @return 该表下所有行的查询结果，若该表不存在，则返回null。
     */
    public List<RowResult> getBatchGetRowResult(String tableName) {
        return tableToRowsResult.get(tableName);
    }

    /**
     * 返回所有表所有行的查询结果。
     * @return 本次BatchGetRow操作返回的结果。
     */
    public Map<String, List<RowResult>> getTableToRowsResult() {
        return tableToRowsResult;
    }

    /**
     * 获取所有查询操作执行失败的行。
     *
     * @return 若存在执行失败的行，则返回所有行，否则返回空列表
     */
    public List<RowResult> getFailedRows() {
        List<RowResult> result = new ArrayList<RowResult>();
        getResult(null, result);
        return result;
    }

    /**
     * 获取所有查询操作执行成功的行。
     *
     * @return 若存在执行成功的行，则返回所有行，否则返回空列表
     */
    public List<RowResult> getSucceedRows() {
        List<RowResult> result = new ArrayList<RowResult>();
        getResult(result, null);
        return result;
    }

    /**
     * 获取所有执行成功过的行以及所有执行失败的行。
     *
     * @param succeedRows 所有执行成功的行
     * @param failedRows 所有执行失败的行
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
     * 是否所有行查询操作都执行成功。
     *
     * @return 若所有行查询操作都执行成功，则返回true，否则返回false
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
