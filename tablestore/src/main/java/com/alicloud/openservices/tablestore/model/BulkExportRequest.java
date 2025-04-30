package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.tunnel.BulkExportQueryCriteria;

public class BulkExportRequest implements Request {

    /**
     * The condition for range queries.
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
     * Get the conditions for a range query.
     * @return The conditions for a range query.
     */
    public BulkExportQueryCriteria getBulkExportQueryCriteria() {
        return bulkExportQueryCriteria;
    }

    /**
     * Set the criteria for range queries.
     * @param bulkExportQueryCriteria The criteria for range queries.
     */

    public void setBulkExportQueryCriteria(BulkExportQueryCriteria bulkExportQueryCriteria) {
        Preconditions.checkNotNull(bulkExportQueryCriteria, "The bulkExportQueryCriteria should not be null.");

        this.bulkExportQueryCriteria = bulkExportQueryCriteria;
    }
}
