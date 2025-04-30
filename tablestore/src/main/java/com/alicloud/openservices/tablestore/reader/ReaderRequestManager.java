package com.alicloud.openservices.tablestore.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.RowQueryCriteria;

public class ReaderRequestManager {

    private final AsyncClientInterface ots;
    private final TableStoreReaderConfig config;
    private final Semaphore callbackSemaphore;
    private final Executor executor;
    private final Semaphore bucketSemaphore;
    private final ReaderStatistics statistics;
    // Store all the PrimaryKeys since the last makeRequest().
    private final Map<String, MultiRowQueryCriteria> criteriaMap;
    // Store table-level RowQueryCriteria information, such as columnsToGet, MaxVersion, etc.
    private final Map<String, RowQueryCriteria> criteriaSetting;
    private final Map<String, List<ReaderGroup>> groupMap;
    private ReaderCallbackFactory callbackFactory;
    private int totalPksCount;

    public ReaderRequestManager(
            AsyncClientInterface ots,
            TableStoreReaderConfig config,
            Semaphore callbackSemaphore,
            TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback,
            Executor executor,
            Semaphore bucketSemaphore,
            ReaderStatistics statistics) {
        this.ots = ots;
        this.config = config;
        this.callbackSemaphore = callbackSemaphore;
        this.executor = executor;
        this.bucketSemaphore = bucketSemaphore;
        this.statistics = statistics;

        callbackFactory = new ReaderCallbackFactory(ots, callbackSemaphore, callback, executor, bucketSemaphore, statistics);

        totalPksCount = 0;
        criteriaMap = new HashMap<String, MultiRowQueryCriteria>();
        criteriaSetting = new HashMap<String, RowQueryCriteria>();
        groupMap = new HashMap<String, List<ReaderGroup>>();
    }

    public int getTotalPksCount() {
        return totalPksCount;
    }

    public ReqWithGroups makeRequest() {
        if (criteriaMap.size() > 0) {
            BatchGetRowRequest request = new BatchGetRowRequest();

            for (Map.Entry<String, MultiRowQueryCriteria> entry : criteriaMap.entrySet()) {
                request.addMultiRowQueryCriteria(entry.getValue());
            }
            criteriaMap.clear();
            totalPksCount = 0;

            Map<String, List<ReaderGroup>> groupsCopy = deepCopyMap();
            groupMap.clear();

            return new ReqWithGroups(request, groupsCopy);
        }

        return null;
    }


    public boolean appendPrimaryKey(PkWithGroup pkWithGroup) {
        String tableName = pkWithGroup.primaryKeyWithTable.getTableName();
        // If the cached data in memory has reached MaxBatchRowsCount
        if (totalPksCount >= config.getMaxBatchRowsCount()) {
            return false;
        }
        prepareCriteriaMap(tableName);

        criteriaMap.get(tableName).addRow(pkWithGroup.primaryKeyWithTable.getPrimaryKey());
        groupMap.get(tableName).add(pkWithGroup.readerGroup);

        totalPksCount += 1;
        return true;
    }

    public void sendRequest(ReqWithGroups reqWithGroups) {
        BatchGetRowRequest finalRequest = reqWithGroups.getRequest();
        ots.batchGetRow(finalRequest, callbackFactory.newInstance(reqWithGroups.getGroupMap()));
    }

    public void setRowQueryCriteria(RowQueryCriteria rowQueryCriteria) {
        String tableName = rowQueryCriteria.getTableName();
        this.criteriaSetting.put(tableName, rowQueryCriteria);
        if (criteriaMap.containsKey(tableName)) {
            criteriaMap.get(tableName).clearColumnsToGet();
            rowQueryCriteria.copyTo(criteriaMap.get(tableName));
        }
    }

    public void setCallback(TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback) {
        callbackFactory = new ReaderCallbackFactory(ots, callbackSemaphore, callback, executor, bucketSemaphore, statistics);
    }

    private void prepareCriteriaMap(String tableName) {
        if (!criteriaMap.containsKey(tableName)) {
            MultiRowQueryCriteria multiRowQueryCriteria = new MultiRowQueryCriteria(tableName);
            multiRowQueryCriteria.setMaxVersions(config.getDefaultMaxVersions());
            if (criteriaSetting.containsKey(tableName)) {
                multiRowQueryCriteria.clearColumnsToGet();
                criteriaSetting.get(tableName).copyTo(multiRowQueryCriteria);
            }
            criteriaMap.put(tableName, multiRowQueryCriteria);
        }
        if (!groupMap.containsKey(tableName)) {
            groupMap.put(tableName, new ArrayList<ReaderGroup>());
        }
    }

    private Map<String, List<ReaderGroup>> deepCopyMap() {
        Map<String, List<ReaderGroup>> copy = new HashMap<String, List<ReaderGroup>>();
        for (Map.Entry<String, List<ReaderGroup>> entry : groupMap.entrySet()) {
            copy.put(entry.getKey(), new ArrayList<ReaderGroup>(entry.getValue()));
        }
        return copy;
    }
}
