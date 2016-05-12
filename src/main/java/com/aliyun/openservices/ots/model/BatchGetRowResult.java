/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
 */

package com.aliyun.openservices.ots.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchGetRowResult extends OTSResult {
    /**
     * BatchGetRow批量操作中单行查询的结果。
     * 若isSucceed为true，则代表该行查询操作成功，可以通过getRow获取单行查询的结果。
     * 若isSucceed为false，则代表该行查询操作失败，可以通过getError获取失败的错误信息。
     */
    public static class RowStatus {
        private boolean isSucceed = false;
        private String tableName;
        private Error error;
        private Row row;
        private ConsumedCapacity consumedCapacity;
        private int index;
        
        public RowStatus(String tableName, Error error, int index) {
            this.isSucceed = false;
            this.tableName = tableName;
            this.error = error;
            this.index = index;
        }
        
        public RowStatus(String tableName, Row row, ConsumedCapacity consumedCapacity, int index) {
            this.isSucceed = true;
            this.tableName = tableName;
            this.row = row;
            this.consumedCapacity = consumedCapacity;
            this.index = index;
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
    }

    /**
     * 批量查询的返回结果。
     */
    private Map<String, List<RowStatus>> tableToRowsStatus;

    /**
     * internal use
     */
    public BatchGetRowResult(OTSResult meta) {
        super(meta);
        this.tableToRowsStatus = new HashMap<String, List<RowStatus>>();
    }

    /**
     * internal use
     */
    public void addResult(RowStatus status) {
        String tableName = status.getTableName();
        List<RowStatus> tableRowStatus = tableToRowsStatus.get(tableName);
        if (tableRowStatus == null) {
            tableRowStatus = new ArrayList<RowStatus>();
            tableToRowsStatus.put(tableName, tableRowStatus);
        }
        tableRowStatus.add(status);
    }
    
    /**
     * 获取某个表的所有行查询结果。
     * @param tableName 表的名称
     * @return 该表下所有行的查询结果，若该表不存在，则返回null。
     */
    public List<RowStatus> getBatchGetRowStatus(String tableName) {
        return tableToRowsStatus.get(tableName);
    }

    /**
     * 返回所有表所有行的查询结果。
     * @return 本次BatchGetRow操作返回的结果。
     */
    public Map<String, List<RowStatus>> getTableToRowsStatus() {
        return tableToRowsStatus;
    }

    /**
     * 获取所有查询操作执行失败的行。
     *
     * @return 若存在执行失败的行，则返回所有行，否则返回空列表
     */
    public List<RowStatus> getFailedRows() {
        List<RowStatus> result = new ArrayList<RowStatus>();
        getResult(null, result);
        return result;
    }

    /**
     * 获取所有查询操作执行成功的行。
     *
     * @return 若存在执行成功的行，则返回所有行，否则返回空列表
     */
    public List<RowStatus> getSucceedRows() {
        List<RowStatus> result = new ArrayList<RowStatus>();
        getResult(result, null);
        return result;
    }

    /**
     * 获取所有执行成功过的行以及所有执行失败的行。
     *
     * @param succeedRows 所有执行成功的行
     * @param failedRows 所有执行失败的行
     */
    public void getResult(List<RowStatus> succeedRows, List<RowStatus> failedRows) {
        for (Map.Entry<String, List<RowStatus>> entry : tableToRowsStatus.entrySet()) {
            for (RowStatus rs : entry.getValue()) {
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
}
