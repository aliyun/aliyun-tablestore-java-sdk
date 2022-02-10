package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;

public class BulkExportRequest implements Request {

    /**
     * 范围查询的条件。
     */
    private BulkExportQueryCriteria bulkExportQueryCriteria;

    public BulkExportRequest() {
    }

    public BulkExportRequest(BulkExportQueryCriteria bulkExportQueryCriteria) {
        setBulkExportQueryCriteria(bulkExportQueryCriteria);
    }

    public String getOperationName() {
        return OperationNames.OP_BULK_EXPORT;
    }

    /**
     * 获取范围查询的条件。
     * @return 范围查询的条件。
     */
    public BulkExportQueryCriteria getBulkExportQueryCriteria() {
        return bulkExportQueryCriteria;
    }

    /**
     * 设置范围查询的条件。
     * @param bulkExportQueryCriteria 范围查询的条件。
     */

    public void setBulkExportQueryCriteria(BulkExportQueryCriteria bulkExportQueryCriteria) {
        Preconditions.checkNotNull(bulkExportQueryCriteria, "The bulkExportQueryCriteria should not be null.");

        this.bulkExportQueryCriteria = bulkExportQueryCriteria;
    }
}
