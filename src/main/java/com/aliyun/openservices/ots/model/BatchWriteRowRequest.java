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

import static com.aliyun.openservices.ots.utils.CodingUtils.assertParameterNotNull;

public class BatchWriteRowRequest {
    
    private Map<String, List<RowPutChange>> rowPutChangesGroupByTable;
    
    private Map<String, List<RowUpdateChange>> rowUpdateChangesGroupByTable;
    
    private Map<String, List<RowDeleteChange>> rowDeleteChangesGroupByTable;
    
    public BatchWriteRowRequest() {
        rowPutChangesGroupByTable = new HashMap<String, List<RowPutChange>>();
        rowUpdateChangesGroupByTable = new HashMap<String, List<RowUpdateChange>>();
        rowDeleteChangesGroupByTable = new HashMap<String, List<RowDeleteChange>>();
    }
    
    /**
     * 添加某个表的PutRow参数。在BatchWriteRowRequest中，相同表的PutRow参数会被组织在一起。
     * @param rowPutChange PutRow参数
     */
    public void addRowPutChange(RowPutChange rowPutChange) {
        assertParameterNotNull(rowPutChange, "rowPutChange");
        String tableName = rowPutChange.getTableName();
        List<RowPutChange> rowPutChanges = rowPutChangesGroupByTable.get(tableName);
        if (rowPutChanges == null) {
            rowPutChanges = new ArrayList<RowPutChange>();
            rowPutChangesGroupByTable.put(tableName, rowPutChanges);
        }
        rowPutChanges.add(rowPutChange);
    }
    
    /**
     * 根据表名和索引返回PutRow参数。
     * BatchWriteRowResult中返回的多行结果允许部分成功部分失败，返回结果按表组织，且表内行的顺序与BatchWriteRowRequest中一一对应。
     * 若用户需要对BatchWriteRowResult中部分失败的行进行重试，则可以根据失败的行所在的表的表名以及在其在返回结果列表内的索引，从BatchWriteRowRequest中反查即可得到请求参数。
     * @param tableName 表的名称
     * @param index 该行在参数列表中得索引
     * @return PutRow参数
     */
    public RowPutChange getRowPutChange(String tableName, int index) {
        List<RowPutChange> rowPutChanges = rowPutChangesGroupByTable.get(tableName);
        if (rowPutChanges == null) {
            return null;
        }
        
        if (index >= rowPutChanges.size()) {
            return null;
        }
        return rowPutChanges.get(index);
    }
    
    /**
     * 获取所有表的PutRow参数。
     * @return 所有表的PutRow参数。
     */
    public Map<String, List<RowPutChange>> getRowPutChange() {
        return rowPutChangesGroupByTable;
    }

    public void addRowChange(RowChange rowChange) {
        if (rowChange instanceof RowPutChange) {
            addRowPutChange((RowPutChange) rowChange);
        } else if (rowChange instanceof RowUpdateChange) {
            addRowUpdateChange((RowUpdateChange) rowChange);
        } else {
            addRowDeleteChange((RowDeleteChange) rowChange);
        }
    }

    /**
     * 添加某个表的UpdateRow参数。在BatchWriteRowRequest中，相同表的UpdateRow参数会被组织在一起。
     * @param rowUpdateChange UpdateRow参数
     */
    public void addRowUpdateChange(RowUpdateChange rowUpdateChange) {
        assertParameterNotNull(rowUpdateChange, "rowUpdateChange");
        String tableName = rowUpdateChange.getTableName();
        List<RowUpdateChange> rowUpdateChanges = rowUpdateChangesGroupByTable.get(tableName);
        if (rowUpdateChanges == null) {
            rowUpdateChanges = new ArrayList<RowUpdateChange>();
            rowUpdateChangesGroupByTable.put(tableName, rowUpdateChanges);
        }
        rowUpdateChanges.add(rowUpdateChange);
    }
    
    /**
     * 根据表名和索引返回UpdateRow参数。
     * BatchWriteRowResult中返回的多行结果允许部分成功部分失败，返回结果按表组织，且表内行的顺序与BatchWriteRowRequest中一一对应。
     * 若用户需要对BatchWriteRowResult中部分失败的行进行重试，则可以根据失败的行所在的表的表名以及在其在返回结果列表内的索引，从BatchWriteRowRequest中反查即可得到请求参数。
     * @param tableName 表的名称
     * @param index 该行在参数列表中得索引
     * @return UpdateRow参数
     */
    public RowUpdateChange getRowUpdateChange(String tableName, int index) {
        List<RowUpdateChange> rowUpdateChanges = rowUpdateChangesGroupByTable.get(tableName);
        if (rowUpdateChanges == null) {
            return null;
        }
        
        if (index >= rowUpdateChanges.size()) {
            return null;
        }
        return rowUpdateChanges.get(index);
    }
    
    /**
     * 获取所有表的UpdateRow参数。
     * @return 所有表的UpdateRow参数。
     */
    public Map<String, List<RowUpdateChange>> getRowUpdateChange() {
        return rowUpdateChangesGroupByTable;
    }
    
    /**
     * 添加某个表的DeleteRow参数。在BatchWriteRowRequest中，相同表的DeleteRow参数会被组织在一起。
     * @param rowDeleteChange DeleteRow参数
     */
    public void addRowDeleteChange(RowDeleteChange rowDeleteChange) {
        assertParameterNotNull(rowDeleteChange, "rowDeleteChange");
        String tableName = rowDeleteChange.getTableName();
        List<RowDeleteChange> rowDeleteChanges = rowDeleteChangesGroupByTable.get(tableName);
        if (rowDeleteChanges == null) {
            rowDeleteChanges = new ArrayList<RowDeleteChange>();
            rowDeleteChangesGroupByTable.put(tableName, rowDeleteChanges);
        }
        rowDeleteChanges.add(rowDeleteChange);
    }
    
    /**
     * 根据表名和索引返回DeleteRow参数。
     * BatchWriteRowResult中返回的多行结果允许部分成功部分失败，返回结果按表组织，且表内行的顺序与BatchWriteRowRequest中一一对应。
     * 若用户需要对BatchWriteRowResult中部分失败的行进行重试，则可以根据失败的行所在的表的表名以及在其在返回结果列表内的索引，从BatchWriteRowRequest中反查即可得到请求参数。
     * @param tableName 表的名称
     * @param index 该行在参数列表中得索引
     * @return DeleteRow参数
     */
    public RowDeleteChange getRowDeleteChange(String tableName, int index) {
        List<RowDeleteChange> rowDeleteChanges = rowDeleteChangesGroupByTable.get(tableName);
        if (rowDeleteChanges == null) {
            return null;
        }
        
        if (index >= rowDeleteChanges.size()) {
            return null;
        }
        return rowDeleteChanges.get(index);
    }
    
    /**
     * 获取所有表的DeleteRow参数。
     * @return 所有表的DeleteRow参数。
     */
    public Map<String, List<RowDeleteChange>> getRowDeleteChange() {
        return rowDeleteChangesGroupByTable;
    }

    /**
     * 检查BatchWriteRowRequest是否包含行。
     *
     * @return true if there is no rows
     */
    public boolean isEmpty() {
        return rowPutChangesGroupByTable.isEmpty() && rowUpdateChangesGroupByTable.isEmpty() && rowDeleteChangesGroupByTable.isEmpty();
    }

    /**
     * 获取该BatchWriteRow请求中包含的总的行数。
     *
     * @return 总的行数
     */
    public int getRowsCount() {
        int rowsCount = 0;
        for (Map.Entry<String, List<RowPutChange>> entry : rowPutChangesGroupByTable.entrySet()) {
            rowsCount += entry.getValue().size();
        }

        for (Map.Entry<String, List<RowUpdateChange>> entry : rowUpdateChangesGroupByTable.entrySet()) {
            rowsCount += entry.getValue().size();
        }

        for (Map.Entry<String, List<RowDeleteChange>> entry : rowDeleteChangesGroupByTable.entrySet()) {
            rowsCount += entry.getValue().size();
        }
        return rowsCount;
    }

    /**
     * 根据请求返回的结果，提取执行失败的行重新构造一次新的请求。
     *
     * @param failedRowsToPut    执行PutRow操作失败的行
     * @param failedRowsToUpdate 执行UpdateRow操作失败的行
     * @param failedRowsToDelete 执行DeleteRow操作失败的行
     * @return 新的用于重试的请求
     */
    public BatchWriteRowRequest createRequestForRetry(List<BatchWriteRowResult.RowStatus> failedRowsToPut,
                                                      List<BatchWriteRowResult.RowStatus> failedRowsToUpdate,
                                                      List<BatchWriteRowResult.RowStatus> failedRowsToDelete) {
        BatchWriteRowRequest request = new BatchWriteRowRequest();
        if (failedRowsToPut != null) {
            for (BatchWriteRowResult.RowStatus rowResult : failedRowsToPut) {
                RowPutChange rowChange = getRowPutChange(rowResult.getTableName(), rowResult.getIndex());
                if (rowChange == null) {
                    throw new IllegalArgumentException("Can not find item in table '" + rowResult.getTableName() + "' " +
                            "with index " + rowResult.getIndex());
                }
                request.addRowPutChange(rowChange);
            }
        }

        if (failedRowsToUpdate != null) {
            for (BatchWriteRowResult.RowStatus rowResult : failedRowsToUpdate) {
                RowUpdateChange rowChange = getRowUpdateChange(rowResult.getTableName(), rowResult.getIndex());
                if (rowChange == null) {
                    throw new IllegalArgumentException("Can not find item in table '" + rowResult.getTableName() + "' " +
                            "with index " + rowResult.getIndex());
                }
                request.addRowUpdateChange(rowChange);
            }
        }

        if (failedRowsToDelete != null) {
            for (BatchWriteRowResult.RowStatus rowResult : failedRowsToDelete) {
                RowDeleteChange rowChange = getRowDeleteChange(rowResult.getTableName(), rowResult.getIndex());
                if (rowChange == null) {
                    throw new IllegalArgumentException("Can not find item in table '" + rowResult.getTableName() + "' " +
                            "with index " + rowResult.getIndex());
                }
                request.addRowDeleteChange(rowChange);
            }
        }

        return request;
    }
}
