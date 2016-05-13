package com.aliyun.openservices.ots.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchWriteRowResult extends OTSResult {
    /**
     * BatchWriteRow批量操作中单行写的结果。
     * 若isSucceed为true，则代表该行写操作成功。
     * 若isSucceed为false，则代表该行写操作失败，可以通过getError获取失败的错误信息。
     */
    public static class RowStatus {
        private boolean isSucceed = false;
        private Error error;
        private ConsumedCapacity consumedCapacity;
        private String tableName;
        private int index;

        public RowStatus(String tableName, ConsumedCapacity consumedCapacity, int index) {
            this.isSucceed = true;
            this.tableName = tableName;
            this.consumedCapacity = consumedCapacity;
            this.index = index;
        }

        public RowStatus(String tableName, Error error, int index) {
            this.isSucceed = false;
            this.tableName = tableName;
            this.error = error;
            this.index = index;
        }

        public boolean isSucceed() {
            return isSucceed;
        }

        public Error getError() {
            return error;
        }

        public String getTableName() {
            return tableName;
        }

        public ConsumedCapacity getConsumedCapacity() {
            return consumedCapacity;
        }

        public int getIndex() {
            return index;
        }
    }

    private Map<String, List<RowStatus>> tableToPutRowStatus;
    private Map<String, List<RowStatus>> tableToUpdateRowStatus;
    private Map<String, List<RowStatus>> tableToDeleteRowStatus;

    /**
     * internal use
     */
    public BatchWriteRowResult(OTSResult meta) {
        super(meta);
        this.tableToPutRowStatus = new HashMap<String, List<RowStatus>>();
        this.tableToUpdateRowStatus = new HashMap<String, List<RowStatus>>();
        this.tableToDeleteRowStatus = new HashMap<String, List<RowStatus>>();
    }

    /**
     * internal use
     */
    public void addPutRowResult(RowStatus status) {
        addResult(status, tableToPutRowStatus);
    }

    /**
     * internal use
     */
    public void addUpdateRowResult(RowStatus status) {
        addResult(status, tableToUpdateRowStatus);
    }

    /**
     * internal use
     */
    public void addDeleteRowResult(RowStatus status) {
        addResult(status, tableToDeleteRowStatus);
    }

    private void addResult(RowStatus status,
                           Map<String, List<RowStatus>> tableToRowStatus) {
        String tableName = status.getTableName();

        List<RowStatus> statuses = tableToRowStatus.get(tableName);
        if (statuses == null) {
            statuses = new ArrayList<RowStatus>();
            tableToRowStatus.put(tableName, statuses);
        }
        statuses.add(status);
    }

    /**
     * 获取某个表上所有PutRow的返回结果。
     *
     * @return PutRow的返回结果，若该表不存在，则返回null。
     */
    public List<RowStatus> getPutRowStatus(String tableName) {
        return tableToPutRowStatus.get(tableName);
    }

    /**
     * 获取所有表上PutRow的返回结果。
     *
     * @return 所有表PutRow返回结果。
     */
    public Map<String, List<RowStatus>> getPutRowStatus() {
        return tableToPutRowStatus;
    }

    /**
     * 获取某个表上所有UpdateRow的返回结果。
     *
     * @return UpdateRow的返回结果，若该表不存在，则返回null。
     */
    public List<RowStatus> getUpdateRowStatus(String tableName) {
        return tableToUpdateRowStatus.get(tableName);
    }

    /**
     * 获取所有表上UpdateRow的返回结果。
     *
     * @return 所有表UpdateRow返回结果。
     */
    public Map<String, List<RowStatus>> getUpdateRowStatus() {
        return tableToUpdateRowStatus;
    }

    /**
     * 获取某个表上所有DeleteRow的返回结果。
     *
     * @return DeleteRow的返回结果，若该表不存在，则返回null。
     */
    public List<RowStatus> getDeleteRowStatus(String tableName) {
        return tableToDeleteRowStatus.get(tableName);
    }

    /**
     * 获取所有表上PutRow的返回结果。
     *
     * @return 所有表PutRow返回结果。
     */
    public Map<String, List<RowStatus>> getDeleteRowStatus() {
        return tableToDeleteRowStatus;
    }

    /**
     * 获取所有PutRow操作执行失败的行。
     *
     * @return 若存在执行失败的行，则返回所有行，否则返回空列表
     */
    public List<RowStatus> getFailedRowsOfPut() {
        List<RowStatus> result = new ArrayList<RowStatus>();
        getResultOfPut(null, result);
        return result;
    }

    /**
     * 获取所有PutRow操作执行成功的行。
     *
     * @return 若存在执行成功的行，则返回所有行，否则返回空列表
     */
    public List<RowStatus> getSucceedRowsOfPut() {
        List<RowStatus> result = new ArrayList<RowStatus>();
        getResultOfPut(result, null);
        return result;
    }

    /**
     * 获取所有PutRow执行成功过的行以及所有执行失败的行。
     *
     * @param succeedRows 所有执行成功的行
     * @param failedRows  所有执行失败的行
     */
    public void getResultOfPut(List<RowStatus> succeedRows, List<RowStatus> failedRows) {
        for (Map.Entry<String, List<RowStatus>> entry : tableToPutRowStatus.entrySet()) {
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
     * 获取所有UpdateRow操作执行失败的行。
     *
     * @return 若存在执行失败的行，则返回所有行，否则返回空列表
     */
    public List<RowStatus> getFailedRowsOfUpdate() {
        List<RowStatus> result = new ArrayList<RowStatus>();
        getResultOfUpdate(null, result);
        return result;
    }

    /**
     * 获取所有UpdateRow操作执行成功的行。
     *
     * @return 若存在执行成功的行，则返回所有行，否则返回空列表
     */
    public List<RowStatus> getSucceedRowsOfUpdate() {
        List<RowStatus> result = new ArrayList<RowStatus>();
        getResultOfUpdate(result, null);
        return result;
    }

    /**
     * 获取所有UpdateRow执行成功过的行以及所有执行失败的行。
     *
     * @param succeedRows 所有执行成功的行
     * @param failedRows  所有执行失败的行
     */
    public void getResultOfUpdate(List<RowStatus> succeedRows, List<RowStatus> failedRows) {
        for (Map.Entry<String, List<RowStatus>> entry : tableToUpdateRowStatus.entrySet()) {
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
     * 获取所有DeleteRow操作执行失败的行。
     *
     * @return 若存在执行失败的行，则返回所有行，否则返回空列表
     */
    public List<RowStatus> getFailedRowsOfDelete() {
        List<RowStatus> result = new ArrayList<RowStatus>();
        getResultOfDelete(null, result);
        return result;
    }

    /**
     * 获取所有DeleteRow操作执行成功的行。
     *
     * @return 若存在执行成功的行，则返回所有行，否则返回空列表
     */
    public List<RowStatus> getSucceedRowsOfDelete() {
        List<RowStatus> result = new ArrayList<RowStatus>();
        getResultOfDelete(result, null);
        return result;
    }

    /**
     * 获取所有DeleteRow执行成功过的行以及所有执行失败的行。
     *
     * @param succeedRows 所有执行成功的行
     * @param failedRows  所有执行失败的行
     */
    public void getResultOfDelete(List<RowStatus> succeedRows, List<RowStatus> failedRows) {
        for (Map.Entry<String, List<RowStatus>> entry : tableToDeleteRowStatus.entrySet()) {
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
     * 是否所有行修改操作都执行成功。
     *
     * @return 若所有行修改操作都执行成功，则返回true，否则返回false
     */
    public boolean isAllSucceed() {
        return getFailedRowsOfPut().isEmpty() && getFailedRowsOfUpdate().isEmpty() && getFailedRowsOfDelete().isEmpty();
    }
}