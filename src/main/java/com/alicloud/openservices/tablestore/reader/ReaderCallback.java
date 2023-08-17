package com.alicloud.openservices.tablestore.reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.SingleRowQueryCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderCallback<Req, Res> implements TableStoreCallback<Req, Res> {
    public static AtomicLong counter = new AtomicLong(0);
    private final AsyncClientInterface ots;
    private final AtomicInteger count;
    private final Semaphore semaphore;
    private final Executor executor;
    private final Semaphore bucketSemaphore;
    private final ReaderStatistics statistics;
    private final Map<String, List<ReaderGroup>> groupMap;
    private final Logger logger = LoggerFactory.getLogger(ReaderCallback.class);
    private final TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback;

    public ReaderCallback(
            AsyncClientInterface ots,
            AtomicInteger count,
            Semaphore semaphore,
            TableStoreCallback<PrimaryKeyWithTable, RowReadResult> callback,
            Executor executor,
            Semaphore bucketSemaphore,
            ReaderStatistics statistics,
            Map<String, List<ReaderGroup>> groupMap) {
        this.ots = ots;
        this.count = count;
        this.semaphore = semaphore;
        this.callback = callback;
        this.executor = executor;
        this.bucketSemaphore = bucketSemaphore;
        this.statistics = statistics;
        this.groupMap = groupMap;
    }

    @Override
    public void onCompleted(Req req, Res res) {
        if (req instanceof BatchGetRowRequest) {

            BatchGetRowRequest request = (BatchGetRowRequest) req;
            BatchGetRowResponse result = (BatchGetRowResponse) res;
            List<BatchGetRowResponse.RowResult> succeed = new ArrayList<BatchGetRowResponse.RowResult>();
            List<BatchGetRowResponse.RowResult> failed = new ArrayList<BatchGetRowResponse.RowResult>();

            result.getResult(succeed, failed);

            for (BatchGetRowResponse.RowResult status : succeed) {
                ReaderGroup group = groupMap.get(status.getTableName()).get(status.getIndex());
                PrimaryKey primaryKey = request.getPrimaryKey(status.getTableName(), status.getIndex());
                triggerSucceedCallback(status, group, primaryKey);
            }

            for (BatchGetRowResponse.RowResult status : failed) {
                ReaderGroup group = groupMap.get(status.getTableName()).get(status.getIndex());
                PrimaryKey primaryKey = request.getPrimaryKey(status.getTableName(), status.getIndex());
                triggerFailedCallback(status, result.getRequestId(), group, primaryKey);
            }
        } else if (req instanceof GetRowRequest) {
            GetRowRequest request = (GetRowRequest) req;
            GetRowResponse result = (GetRowResponse) res;
            String tableName = request.getRowQueryCriteria().getTableName();
            BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(tableName, result.getRow(), result.getConsumedCapacity(), 0);
            triggerSucceedCallback(rowResult, groupMap.get(tableName).get(0), request.getRowQueryCriteria().getPrimaryKey());
        }

        requestComplete();
    }

    @Override
    public void onFailed(Req req, Exception ex) {
        if (ex instanceof TableStoreException) {
            failedOnException(req, (TableStoreException) ex);
        } else {
            failedOnUnknownException(req, ex);
        }

        requestComplete();
    }

    private void triggerSucceedCallback(final BatchGetRowResponse.RowResult rowResult, final ReaderGroup group, final PrimaryKey primaryKey) {
        statistics.totalSucceedRowsCount.incrementAndGet();
        group.succeedOneRow(primaryKey, rowResult);
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                callback.onCompleted(new PrimaryKeyWithTable(rowResult.getTableName(), primaryKey), new RowReadResult(primaryKey, rowResult));
            }
        });
    }

    private void triggerFailedCallback(final BatchGetRowResponse.RowResult rowResult, String requestID, final ReaderGroup group, final PrimaryKey primaryKey) {
        statistics.totalFailedRowsCount.incrementAndGet();
        final Exception exception = new TableStoreException(rowResult.getError().getMessage(), null, rowResult.getError().getCode(), requestID, 0);
        group.failedOneRow(primaryKey, rowResult, exception);
        logger.error("GetRow Failed， PK: {}, error: {}.", primaryKey, rowResult.getError());
        if (callback == null) {
            return;
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                callback.onFailed(
                        new PrimaryKeyWithTable(rowResult.getTableName(), primaryKey),
                        exception);
            }
        });
    }

    public void failedOnException(Req request, TableStoreException ex) {
        logger.debug("OnFailed on TableStoreException:", ex);
        if (request instanceof BatchGetRowRequest) {
            BatchGetRowRequest batchGetRowRequest = (BatchGetRowRequest) request;
            retryBatchGet(batchGetRowRequest);
        } else if (request instanceof GetRowRequest) {
            String tableName = ((GetRowRequest) request).getRowQueryCriteria().getTableName();
            BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(tableName, new Error(ex.getErrorCode(), ex.getMessage()), 0);
            triggerFailedCallback(rowResult, ex.getRequestId(), groupMap.get(tableName).get(0), ((GetRowRequest) request).getRowQueryCriteria().getPrimaryKey());
        }
    }

    public void failedOnUnknownException(Req req, final Exception ex) {
        logger.debug("OnFailed on ClientException: ", ex);
        if (req instanceof BatchGetRowRequest) {
            final BatchGetRowRequest request = (BatchGetRowRequest) req;

            for (Map.Entry<String, MultiRowQueryCriteria> entry : request.getCriteriasByTable().entrySet()) {
                statistics.totalFailedRowsCount.addAndGet(entry.getValue().size());
                for (int i = 0; i < entry.getValue().size(); i++) {
                    PrimaryKey primaryKey = entry.getValue().get(i);
                    BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(entry.getKey(), new Error("FailedOnUnknownException", ex.getMessage()), i);
                    ReaderGroup group = groupMap.get(entry.getKey()).get(i);

                    group.failedOneRow(primaryKey, rowResult, ex);
                    logger.error("RowChange Failed: ", ex);
                }
            }

            if (callback == null) {
                return;
            }

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    for (Map.Entry<String, MultiRowQueryCriteria> entry : request.getCriteriasByTable().entrySet()) {
                        for (int i = 0; i < entry.getValue().size(); i++) {
                            PrimaryKey primaryKey = entry.getValue().get(i);

                            callback.onFailed(
                                    new PrimaryKeyWithTable(entry.getKey(), primaryKey), ex);
                        }
                    }
                }
            });
        } else if (req instanceof GetRowRequest) {
            String tableName = ((GetRowRequest) req).getRowQueryCriteria().getTableName();
            BatchGetRowResponse.RowResult rowResult = new BatchGetRowResponse.RowResult(tableName, new Error("FailedOnUnknownException", ex.getMessage()), 0);
            triggerFailedCallback(rowResult, null, groupMap.get(tableName).get(0), ((GetRowRequest) req).getRowQueryCriteria().getPrimaryKey());
        }
    }

    private void retryBatchGet(BatchGetRowRequest request) {
        for (Map.Entry<String, MultiRowQueryCriteria> entry : request.getCriteriasByTable().entrySet()) {
            String tableName = entry.getKey();
            for (int i = 0; i < entry.getValue().getRowKeys().size(); i++) {
                PrimaryKey pk = entry.getValue().getRowKeys().get(i);

                SingleRowQueryCriteria singleRowCriteria = new SingleRowQueryCriteria(entry.getKey());
                entry.getValue().copyTo(singleRowCriteria);
                singleRowCriteria.setPrimaryKey(pk);

                GetRowRequest singleGetRequest = new GetRowRequest();
                singleGetRequest.setRowQueryCriteria(singleRowCriteria);

                ReaderGroup group = groupMap.get(tableName).get(i);

                retrySingleBatchGet(singleGetRequest, tableName, group);
            }
        }
    }

    private void retrySingleBatchGet(GetRowRequest singleGetRequest, String tableName, ReaderGroup group) {
        final Map<String, List<ReaderGroup>> subGroupMap = new HashMap<String, List<ReaderGroup>>();
        List<ReaderGroup> list = new ArrayList<ReaderGroup>();
        list.add(group);
        subGroupMap.put(tableName, list);

        statistics.totalSingleRowRequestCount.incrementAndGet();
        statistics.totalRequestCount.incrementAndGet();
        this.count.incrementAndGet();
        ots.getRow(singleGetRequest, new ReaderCallback<GetRowRequest, GetRowResponse>(ots, count, semaphore, callback, executor, bucketSemaphore, statistics, subGroupMap));
    }

    private void requestComplete() {
        int remain = this.count.decrementAndGet();
        if (remain == 0) {
            logger.debug("BucketSemaphore Release: " + counter.incrementAndGet());
            semaphore.release();
            bucketSemaphore.release();
            logger.debug("Release semaphore.");
        }
    }
}
