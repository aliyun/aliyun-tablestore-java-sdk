/**
 * Copyright (C) Alibaba Cloud Computing
 * All rights reserved.
 * 
 * 版权所有 （C）阿里云计算有限公司
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
     * 增加某个表的多行查询参数。若该表已经添加过查询条件，则之前的查询条件会被覆盖。
     * @param criteria 单行查询条件
     */
    public void addMultiRowQueryCriteria(MultiRowQueryCriteria criteria) {
        Preconditions.checkArgument(criteria != null && !criteria.isEmpty(), "The query criteria for table should not be null or empty.");
        String tableName = criteria.getTableName();
        criteriasGroupByTable.put(tableName, criteria);
    }
    
    /**
     * 根据表名和索引返回某一行的主键。
     * BatchGetRowResult中返回的多行结果允许部分成功部分失败，返回结果按表组织，且表内行的顺序与BatchGetRowRequest中一一对应。
     * 若用户需要对BatchGetRowResult中部分失败的GetRow查询进行重试，则可以根据失败的查询所在的表的表名以及在其在返回结果列表内的索引，从BatchGetRowRequest中反查即可得到对应的行的主键。
     * @param tableName 表的名称
     * @param index 该行在参数列表中得索引
     * @return 行的主键
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
     * 获取按表组织的多行查询参数。
     * @return 多行查询参数。
     */
    public Map<String, MultiRowQueryCriteria> getCriteriasByTable() {
        return criteriasGroupByTable;
    }

    /**
     * 获取指定表的多行查询参数。
     *
     * @param tableName 表的名称
     * @return 若该行存在，则返回该行查询参数，否则返回null
     */
    public MultiRowQueryCriteria getCriteria(String tableName) {
        return criteriasGroupByTable.get(tableName);
    }

    /**
     * 根据请求返回的结果创建新的请求用于重试。
     *
     * @param failedRows 返回结果中查询失败的行
     * @return 新的请求
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
