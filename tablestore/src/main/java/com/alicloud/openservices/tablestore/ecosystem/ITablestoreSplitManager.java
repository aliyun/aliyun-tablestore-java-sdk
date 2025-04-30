package com.alicloud.openservices.tablestore.ecosystem;

import com.alicloud.openservices.tablestore.SyncClient;

import java.util.List;

/**
 * interface for TablstoreSplitManager
 */
public interface ITablestoreSplitManager {
    /**
     * @param client
     * @param filter
     * @param tableName
     * @param parameter
     * @param requiredColumns
     * @return
     */
     List<ITablestoreSplit> generateTablestoreSplits(
            SyncClient client,
            Filter filter,
            String tableName,
            ComputeParameters parameter,
            List<String> requiredColumns);
}
