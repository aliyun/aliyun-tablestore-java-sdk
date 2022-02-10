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
     * BatchWriteRow批量操作中单行写的结果。
     * 若isSucceed为true，则代表该行写操作成功。
     * 若isSucceed为false，则代表该行写操作失败，可以通过getError获取失败的错误信息。
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
     * 获取某个表上所有写操作的返回结果。
     *
     * @return 写操作的返回结果，若该表不存在，则返回null。
     */
    public List<BulkImportResponse.RowResult> getRowResults(){
        return rowResults;
    }

    /**
     * 获取所有PutRow操作执行失败的行。
     *
     * @return 若存在执行失败的行，则返回所有行，否则返回空列表
     */
    public List<BulkImportResponse.RowResult> getFailedRows() {
        List<BulkImportResponse.RowResult> result = new ArrayList<BulkImportResponse.RowResult>();
        getResult(null, result);
        return result;
    }

    /**
     * 获取所有操作执行成功的行。
     *
     * @return 若存在执行成功的行，则返回所有行，否则返回空列表
     */
    public List<BulkImportResponse.RowResult> getSucceedRows() {
        List<BulkImportResponse.RowResult> result = new ArrayList<BulkImportResponse.RowResult>();
        getResult(result, null);
        return result;
    }

    /**
     * 获取所有执行成功过的行以及所有执行失败的行。
     *
     * @param succeedRows 所有执行成功的行
     * @param failedRows  所有执行失败的行
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
     * 是否所有行修改操作都执行成功。
     *
     * @return 若所有行修改操作都执行成功，则返回true，否则返回false
     */
    public boolean isAllSucceed() {
        return getFailedRows().isEmpty();
    }
}