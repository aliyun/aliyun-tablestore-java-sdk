/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * Copyright (C) Alibaba Cloud Computing
 */

package com.alicloud.openservices.tablestore.model;

import com.alicloud.openservices.tablestore.core.utils.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchGetRowRequest implements Request {
    private Map<String, MultiRowQueryCriteria> criteriasGroupByTable;
    
    public BatchGetRowRequest() {
        criteriasGroupByTable = new HashMap<String, MultiRowQueryCriteria>();
    }

    public String getOperationName() {
        return OperationNames.OP_BATCH_GET_ROW;
    }

    /**
     * Add multi-row query parameters for a table. If query conditions for the table have already been added, the previous query conditions will be overwritten.
     * @param criteria Single-row query condition
     */
    public void addMultiRowQueryCriteria(MultiRowQueryCriteria criteria) {
        Preconditions.checkArgument(criteria != null && !criteria.isEmpty(), "The query criteria for table should not be null or empty.");
        String tableName = criteria.getTableName();
        criteriasGroupByTable.put(tableName, criteria);
    }
    
    /**
     * Returns the primary key of a specific row based on the table name and index.
     * The multi-row results returned in BatchGetRowResult allow partial success and partial failure, with results organized by table, and the order of rows within the table corresponds one-to-one with the BatchGetRowRequest.
     * If the user needs to retry some failed GetRow queries in the BatchGetRowResult, they can look up the corresponding row's primary key from the BatchGetRowRequest using the table name and its index in the result list where the failed query resides.
     * @param tableName The name of the table
     * @param index The index of the row in the parameter list
     * @return The primary key of the row
     */
    public PrimaryKey getPrimaryKey(String tableName, int index) {
        MultiRowQueryCriteria criteria = criteriasGroupByTable.get(tableName);
        if (criteria == null) {
            return null;
        }
        
        if (index >= criteria.getRowKeys().size()) {
            return null;
        }
        return criteria.getRowKeys().get(index);
    }

    /**
     * Get the multi-row query parameters organized by table.
     * @return Multi-row query parameters.
     */
    public Map<String, MultiRowQueryCriteria> getCriteriasByTable() {
        return criteriasGroupByTable;
    }

    /**
     * Get the multi-row query parameters for the specified table.
     *
     * @param tableName The name of the table
     * @return If the row exists, return the query parameters for that row, otherwise return null
     */
    public MultiRowQueryCriteria getCriteria(String tableName) {
        return criteriasGroupByTable.get(tableName);
    }

    /**
     * Creates a new request for retry based on the returned result.
     *
     * @param failedRows rows that failed to query in the returned result
     * @return a new request
     */
    public BatchGetRowRequest createRequestForRetry(List<BatchGetRowResponse.RowResult> failedRows) {
        BatchGetRowRequest request = new BatchGetRowRequest();
        for (BatchGetRowResponse.RowResult rowResult : failedRows) {
        	PrimaryKey primaryKey = getPrimaryKey(rowResult.getTableName(), rowResult.getIndex());
            if (primaryKey == null) {
                throw new IllegalArgumentException("Can not find table '" + rowResult.getTableName() + "' with index " + rowResult.getIndex());
            }

            MultiRowQueryCriteria newCriteria = request.getCriteria(rowResult.getTableName());

            if (newCriteria == null) {
                MultiRowQueryCriteria oldCriteria = getCriteria(rowResult.getTableName());
                if (oldCriteria == null) {
                    throw new IllegalArgumentException("Can not found query criteria for table '" + rowResult.getTableName() + "'.");
                }
                newCriteria = oldCriteria.cloneWithoutRowKeys();
                newCriteria.addRow(primaryKey);
                request.addMultiRowQueryCriteria(newCriteria);
            } else {
                newCriteria.addRow(primaryKey);
            }
        }
        return request;
    }

    public boolean isEmpty() {
        return criteriasGroupByTable.isEmpty();
    }
}
