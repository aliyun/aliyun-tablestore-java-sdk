package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

/**
 * GetRow的查询参数。
 */
public class GetRowRequest extends TxnRequest {

    /**
     * 单行查询条件。
     */
    private SingleRowQueryCriteria rowQueryCriteria;

    public GetRowRequest() {

    }

    /**
     * 通过单行查询条件构造GetRowRequest对象。
     *
     * @param rowQueryCriteria 单行查询条件。
     */
    public GetRowRequest(SingleRowQueryCriteria rowQueryCriteria) {
        setRowQueryCriteria(rowQueryCriteria);
    }

    @Override
    public String getOperationName() {
        return OperationNames.OP_GET_ROW;
    }

    /**
     * 获取单行查询条件。
     *
     * @return 单行查询条件。
     */
    public SingleRowQueryCriteria getRowQueryCriteria() {
        return rowQueryCriteria;
    }

    /**
     * 设置单行查询条件。
     *
     * @param rowQueryCriteria 单行查询条件。
     */
    public void setRowQueryCriteria(SingleRowQueryCriteria rowQueryCriteria) {
        Preconditions.checkNotNull(rowQueryCriteria, "The row query criteria should not be null.");
        this.rowQueryCriteria = rowQueryCriteria;
    }
}
