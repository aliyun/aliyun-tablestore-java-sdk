package com.alicloud.openservices.tablestore.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchWriteRowResponse extends Response {
    /**
     * BatchWriteRow批量操作中单行写的结果。
     * 若isSucceed为true，则代表该行写操作成功。
     * 若isSucceed为false，则代表该行写操作失败，可以通过getError获取失败的错误信息。
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
         * 判断该行查询是否执行成功。
         * <p>只有在成功的时候，{@link #consumedCapacity}才有效。</p>
         * <p>只有在执行不成功的时候，{@link #error}才有效。</p>
         *
         * @return 若执行成功，则返回true，否则返回false
         */
        public boolean isSucceed() {
            return isSucceed;
        }

        /**
         * 获取该行所在表的名称。
         * <p>若该行查询失败，可以根据表名和索引通过{@link BatchGetRowRequest#getPrimaryKey(String, int)}中获取查询参数进行重试。</p>
         *
         * @return 表的名称
         */
        public String getTableName() {
            return tableName;
        }

        /**
         * 若该行查询执行失败，则返回具体的错误信息。
         *
         * @return 若执行失败，则返回错误信息，否则返回null
         */
        public Error getError() {
            return error;
        }

        /**
         * 若该行查询成功，则返回消耗的能力单元。
         *
         * @return 若执行成功，则返回消耗的能力单元，否则返回null
         */
        public ConsumedCapacity getConsumedCapacity() {
            return consumedCapacity;
        }

        /**
         * 获取该行在{@link BatchGetRowRequest}的多行查询参数的索引位置。
         * <p>若该行查询失败，可以根据表名和索引通过{@link BatchGetRowRequest#getPrimaryKey(String, int)}中获取查询参数进行重试。</p>
         *
         * @return 索引位置
         */
        public int getIndex() {
            return index;
        }

        /**
         *  获取返回的行数据
         * @return 若有返回行数据，则返回行数据，否则返回null
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
     * 获取某个表上所有写操作的返回结果。
     *
     * @return 写操作的返回结果，若该表不存在，则返回null。
     */
    public List<RowResult> getRowStatus(String tableName) {
        return tableToRowStatus.get(tableName);
    }

    /**
     * 获取所有表上写操作的返回结果。
     *
     * @return 所有表写操作返回结果。
     */
    public Map<String, List<RowResult>> getRowStatus() {
        return tableToRowStatus;
    }

    /**
     * 获取所有PutRow操作执行失败的行。
     *
     * @return 若存在执行失败的行，则返回所有行，否则返回空列表
     */
    public List<RowResult> getFailedRows() {
        List<RowResult> result = new ArrayList<RowResult>();
        getResult(null, result);
        return result;
    }

    /**
     * 获取所有操作执行成功的行。
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
     * @param failedRows  所有执行失败的行
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
     * 是否所有行修改操作都执行成功。
     *
     * @return 若所有行修改操作都执行成功，则返回true，否则返回false
     */
    public boolean isAllSucceed() {
        return getFailedRows().isEmpty();
    }
}
