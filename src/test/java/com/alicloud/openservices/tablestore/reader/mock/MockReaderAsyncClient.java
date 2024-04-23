package com.alicloud.openservices.tablestore.reader.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.Future;

import com.alicloud.openservices.tablestore.AsyncClientInterface;
import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClientInterface;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.CallbackImpledFuture;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.model.AbortTransactionRequest;
import com.alicloud.openservices.tablestore.model.AbortTransactionResponse;
import com.alicloud.openservices.tablestore.model.AddDefinedColumnRequest;
import com.alicloud.openservices.tablestore.model.AddDefinedColumnResponse;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse.RowResult;
import com.alicloud.openservices.tablestore.model.BatchWriteRowRequest;
import com.alicloud.openservices.tablestore.model.BatchWriteRowResponse;
import com.alicloud.openservices.tablestore.model.BulkExportRequest;
import com.alicloud.openservices.tablestore.model.BulkExportResponse;
import com.alicloud.openservices.tablestore.model.BulkImportRequest;
import com.alicloud.openservices.tablestore.model.BulkImportResponse;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CommitTransactionRequest;
import com.alicloud.openservices.tablestore.model.CommitTransactionResponse;
import com.alicloud.openservices.tablestore.model.ComputeSplitsBySizeRequest;
import com.alicloud.openservices.tablestore.model.ComputeSplitsBySizeResponse;
import com.alicloud.openservices.tablestore.model.ComputeSplitsRequest;
import com.alicloud.openservices.tablestore.model.ComputeSplitsResponse;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.CreateIndexRequest;
import com.alicloud.openservices.tablestore.model.CreateIndexResponse;
import com.alicloud.openservices.tablestore.model.CreateTableRequest;
import com.alicloud.openservices.tablestore.model.CreateTableResponse;
import com.alicloud.openservices.tablestore.model.DeleteDefinedColumnRequest;
import com.alicloud.openservices.tablestore.model.DeleteDefinedColumnResponse;
import com.alicloud.openservices.tablestore.model.DeleteIndexRequest;
import com.alicloud.openservices.tablestore.model.DeleteIndexResponse;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.DeleteRowResponse;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.DeleteTableResponse;
import com.alicloud.openservices.tablestore.model.DescribeStreamRequest;
import com.alicloud.openservices.tablestore.model.DescribeStreamResponse;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.Error;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.GetRowRequest;
import com.alicloud.openservices.tablestore.model.GetRowResponse;
import com.alicloud.openservices.tablestore.model.GetShardIteratorRequest;
import com.alicloud.openservices.tablestore.model.GetShardIteratorResponse;
import com.alicloud.openservices.tablestore.model.GetStreamRecordRequest;
import com.alicloud.openservices.tablestore.model.GetStreamRecordResponse;
import com.alicloud.openservices.tablestore.model.ListStreamRequest;
import com.alicloud.openservices.tablestore.model.ListStreamResponse;
import com.alicloud.openservices.tablestore.model.ListTableRequest;
import com.alicloud.openservices.tablestore.model.ListTableResponse;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionRequest;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionResponse;
import com.alicloud.openservices.tablestore.model.UpdateRowRequest;
import com.alicloud.openservices.tablestore.model.UpdateRowResponse;
import com.alicloud.openservices.tablestore.model.UpdateTableRequest;
import com.alicloud.openservices.tablestore.model.UpdateTableResponse;
import com.alicloud.openservices.tablestore.model.delivery.CreateDeliveryTaskRequest;
import com.alicloud.openservices.tablestore.model.delivery.CreateDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.delivery.DeleteDeliveryTaskRequest;
import com.alicloud.openservices.tablestore.model.delivery.DeleteDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.delivery.DescribeDeliveryTaskRequest;
import com.alicloud.openservices.tablestore.model.delivery.DescribeDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.delivery.ListDeliveryTaskRequest;
import com.alicloud.openservices.tablestore.model.delivery.ListDeliveryTaskResponse;
import com.alicloud.openservices.tablestore.model.search.CreateSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.CreateSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.DeleteSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DeleteSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.DescribeSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.ListSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.ParallelScanResponse;
import com.alicloud.openservices.tablestore.model.search.SearchRequest;
import com.alicloud.openservices.tablestore.model.search.SearchResponse;
import com.alicloud.openservices.tablestore.model.search.UpdateSearchIndexRequest;
import com.alicloud.openservices.tablestore.model.search.UpdateSearchIndexResponse;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;

public class MockReaderAsyncClient implements AsyncClientInterface {
    public Future<CreateTableResponse> createTable(CreateTableRequest createTableRequest, TableStoreCallback<CreateTableRequest, CreateTableResponse> callback) {
        return null;
    }

    public Future<UpdateTableResponse> updateTable(UpdateTableRequest updateTableRequest, TableStoreCallback<UpdateTableRequest, UpdateTableResponse> callback) {
        return null;
    }

    public Future<DescribeTableResponse> describeTable(DescribeTableRequest describeTableRequest, TableStoreCallback<DescribeTableRequest, DescribeTableResponse> callback) {
        return null;
    }

    public Future<ListTableResponse> listTable(TableStoreCallback<ListTableRequest, ListTableResponse> callback) {
        return null;
    }

    public Future<DeleteTableResponse> deleteTable(DeleteTableRequest deleteTableRequest, TableStoreCallback<DeleteTableRequest, DeleteTableResponse> callback) {
        return null;
    }

    public Future<CreateIndexResponse> createIndex(CreateIndexRequest createIndexRequest, TableStoreCallback<CreateIndexRequest, CreateIndexResponse> callback) {
        return null;
    }

    public Future<DeleteIndexResponse> deleteIndex(DeleteIndexRequest deleteIndexRequest, TableStoreCallback<DeleteIndexRequest, DeleteIndexResponse> callback) {
        return null;
    }

    public Future<AddDefinedColumnResponse> addDefinedColumn(AddDefinedColumnRequest addDefinedColumnRequest, TableStoreCallback<AddDefinedColumnRequest, AddDefinedColumnResponse> callback) {
        return null;
    }

    public Future<DeleteDefinedColumnResponse> deleteDefinedColumn(DeleteDefinedColumnRequest deleteDefinedColumnRequest, TableStoreCallback<DeleteDefinedColumnRequest, DeleteDefinedColumnResponse> callback) {
        return null;
    }

    public Future<GetRowResponse> getRow(GetRowRequest getRowRequest, TableStoreCallback<GetRowRequest, GetRowResponse> callback) {
        PrimaryKey primaryKey = getRowRequest.getRowQueryCriteria().getPrimaryKey();

        if (primaryKey.hashCode() % 2 == 0) {
            callback.onFailed(getRowRequest, new TableStoreException("test error msg", "testErrorCode"));
        } else {
            List<Column> columns = new ArrayList<Column>();
            for (String columnsToGet : getRowRequest.getRowQueryCriteria().getColumnsToGet()) {
                columns.add(new Column(columnsToGet, ColumnValue.fromString("value_of_" + columnsToGet), System.currentTimeMillis()));
            }
            if (getRowRequest.getRowQueryCriteria().getColumnsToGet().isEmpty()) {
                for (int i = 0; i < 10; i++) {
                    columns.add(new Column("col_" + i, ColumnValue.fromString("value_of_col_" + i), System.currentTimeMillis()));
                }
            }
            Row row = new Row(primaryKey, columns);
            GetRowResponse response = new GetRowResponse(new Response(), row, new ConsumedCapacity(new CapacityUnit(1, 0)));
            callback.onCompleted(getRowRequest, response);
        }

        return null;
    }

    public Future<PutRowResponse> putRow(PutRowRequest putRowRequest, TableStoreCallback<PutRowRequest, PutRowResponse> callback) {
        return null;
    }

    public Future<UpdateRowResponse> updateRow(UpdateRowRequest updateRowRequest, TableStoreCallback<UpdateRowRequest, UpdateRowResponse> callback) {
        return null;
    }

    public Future<DeleteRowResponse> deleteRow(DeleteRowRequest deleteRowRequest, TableStoreCallback<DeleteRowRequest, DeleteRowResponse> callback) {
        return null;
    }

    public Future<BatchGetRowResponse> batchGetRow(BatchGetRowRequest batchGetRowRequest, TableStoreCallback<BatchGetRowRequest, BatchGetRowResponse> callback) {
        Response meta = new Response();
        BatchGetRowResponse response = new BatchGetRowResponse(meta);
        for (Entry<String, MultiRowQueryCriteria> entry : batchGetRowRequest.getCriteriasByTable().entrySet()) {
            // 使用不同的tableName，mock不同的返回结果
            if ("normalTable".equals(entry.getKey())) {
                int index = 0;
                for (PrimaryKey pk : entry.getValue().getRowKeys()) {
                    List<Column> columns = new ArrayList<Column>();
                    for (String columnsToGet : entry.getValue().getColumnsToGet()) {
                        columns.add(new Column(columnsToGet, ColumnValue.fromString("value_of_" + columnsToGet), System.currentTimeMillis()));
                    }
                    if (entry.getValue().getColumnsToGet().isEmpty()) {
                        for (int i = 0; i < 10; i++) {
                            columns.add(new Column("col_" + i, ColumnValue.fromString("value_of_col_" + i), System.currentTimeMillis()));
                        }
                    }
                    Row row = new Row(pk, columns);
                    response.addResult(new RowResult(
                            entry.getKey(), // tableName
                            row,
                            new ConsumedCapacity(new CapacityUnit(1, 0)),
                            index
                    ));
                    index += 1;
                }
            } else if ("partitionFailedTable".equals(entry.getKey())) {
                int index = 0;
                for (PrimaryKey pk : entry.getValue().getRowKeys()) {
                    if (pk.hashCode() % 2 == 0) {
                        response.addResult(new RowResult(
                                entry.getKey(), // tableName
                                new Error("test error msg", "testErrorCode"),
                                index
                        ));
                    } else {
                        List<Column> columns = new ArrayList<Column>();
                        for (String columnsToGet : entry.getValue().getColumnsToGet()) {
                            columns.add(new Column(columnsToGet, ColumnValue.fromString("value_of_" + columnsToGet), System.currentTimeMillis()));
                        }
                        if (entry.getValue().getColumnsToGet().isEmpty()) {
                            for (int i = 0; i < 10; i++) {
                                columns.add(new Column("col_" + i, ColumnValue.fromString("value_of_col_" + i), System.currentTimeMillis()));
                            }
                        }
                        Row row = new Row(pk, columns);
                        response.addResult(new RowResult(
                                entry.getKey(), // tableName
                                row,
                                new ConsumedCapacity(new CapacityUnit(1, 0)),
                                index
                        ));
                    }
                    index += 1;
                }
            } else if ("tableStoreExceptionFailedTable".equals(entry.getKey())) {
                callback.onFailed(batchGetRowRequest, new TableStoreException("Test TableStoreException msg", "testErrorCode"));
                return null;
            } else if ("clientExceptionFailedTable".equals(entry.getKey())) {
                callback.onFailed(batchGetRowRequest, new ClientException("Test ClientException msg"));
                return null;
            }
        }
        try {
            Thread.sleep(new Random().nextInt(50));
        } catch (Exception e) {

        }
        CallbackImpledFuture<BatchGetRowRequest, BatchGetRowResponse> batchGetRowResponseFuture = new CallbackImpledFuture<BatchGetRowRequest, BatchGetRowResponse>();
        batchGetRowResponseFuture.onCompleted(null, response);
        callback.onCompleted(batchGetRowRequest, response);

        return batchGetRowResponseFuture;
    }

    public Future<BatchWriteRowResponse> batchWriteRow(BatchWriteRowRequest batchWriteRowRequest, TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse> callback) {
        return null;
    }

    public Future<BulkImportResponse> bulkImport(BulkImportRequest bulkImportRequest, TableStoreCallback<BulkImportRequest, BulkImportResponse> callback) {
        return null;
    }

    public Future<GetRangeResponse> getRange(GetRangeRequest getRangeRequest, TableStoreCallback<GetRangeRequest, GetRangeResponse> callback) {
        return null;
    }

    public Future<BulkExportResponse> bulkExport(BulkExportRequest bulkExportRequest, TableStoreCallback<BulkExportRequest, BulkExportResponse> callback) {
        return null;
    }

    public Future<ComputeSplitsBySizeResponse> computeSplitsBySize(ComputeSplitsBySizeRequest computeSplitsBySizeRequest, TableStoreCallback<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse> callback) {
        return null;
    }

    public Future<ListStreamResponse> listStream(ListStreamRequest listStreamRequest, TableStoreCallback<ListStreamRequest, ListStreamResponse> callback) {
        return null;
    }

    public Future<DescribeStreamResponse> describeStream(DescribeStreamRequest describeStreamRequest, TableStoreCallback<DescribeStreamRequest, DescribeStreamResponse> callback) {
        return null;
    }

    public Future<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest getShardIteratorRequest, TableStoreCallback<GetShardIteratorRequest, GetShardIteratorResponse> callback) {
        return null;
    }

    public Future<GetStreamRecordResponse> getStreamRecord(GetStreamRecordRequest getStreamRecordRequest, TableStoreCallback<GetStreamRecordRequest, GetStreamRecordResponse> callback) {
        return null;
    }

    public Future<StartLocalTransactionResponse> startLocalTransaction(StartLocalTransactionRequest request, TableStoreCallback<StartLocalTransactionRequest, StartLocalTransactionResponse> callback) {
        return null;
    }

    public Future<CommitTransactionResponse> commitTransaction(CommitTransactionRequest request, TableStoreCallback<CommitTransactionRequest, CommitTransactionResponse> callback) {
        return null;
    }

    public Future<AbortTransactionResponse> abortTransaction(AbortTransactionRequest request, TableStoreCallback<AbortTransactionRequest, AbortTransactionResponse> callback) {
        return null;
    }

    public Future<CreateSearchIndexResponse> createSearchIndex(CreateSearchIndexRequest request, TableStoreCallback<CreateSearchIndexRequest, CreateSearchIndexResponse> callback) {
        return null;
    }

    public Future<UpdateSearchIndexResponse> updateSearchIndex(UpdateSearchIndexRequest request, TableStoreCallback<UpdateSearchIndexRequest, UpdateSearchIndexResponse> callback) {
        return null;
    }

    public Future<ListSearchIndexResponse> listSearchIndex(ListSearchIndexRequest request, TableStoreCallback<ListSearchIndexRequest, ListSearchIndexResponse> callback) {
        return null;
    }

    public Future<DeleteSearchIndexResponse> deleteSearchIndex(DeleteSearchIndexRequest request, TableStoreCallback<DeleteSearchIndexRequest, DeleteSearchIndexResponse> callback) {
        return null;
    }

    public Future<DescribeSearchIndexResponse> describeSearchIndex(DescribeSearchIndexRequest request, TableStoreCallback<DescribeSearchIndexRequest, DescribeSearchIndexResponse> callback) {
        return null;
    }

    public Future<ComputeSplitsResponse> computeSplits(ComputeSplitsRequest request, TableStoreCallback<ComputeSplitsRequest, ComputeSplitsResponse> callback) {
        return null;
    }

    public Future<ParallelScanResponse> parallelScan(ParallelScanRequest request, TableStoreCallback<ParallelScanRequest, ParallelScanResponse> callback) {
        return null;
    }

    public Future<CreateDeliveryTaskResponse> createDeliveryTask(CreateDeliveryTaskRequest request, TableStoreCallback<CreateDeliveryTaskRequest, CreateDeliveryTaskResponse> callback) {
        return null;
    }

    public Future<DeleteDeliveryTaskResponse> deleteDeliveryTask(DeleteDeliveryTaskRequest request, TableStoreCallback<DeleteDeliveryTaskRequest, DeleteDeliveryTaskResponse> callback) {
        return null;
    }

    public Future<DescribeDeliveryTaskResponse> describeDeliveryTask(DescribeDeliveryTaskRequest request, TableStoreCallback<DescribeDeliveryTaskRequest, DescribeDeliveryTaskResponse> callback) {
        return null;
    }

    public Future<ListDeliveryTaskResponse> listDeliveryTask(ListDeliveryTaskRequest request, TableStoreCallback<ListDeliveryTaskRequest, ListDeliveryTaskResponse> callback) {
        return null;
    }

    public Future<SearchResponse> search(SearchRequest request, TableStoreCallback<SearchRequest, SearchResponse> callback) {
        return null;
    }

    public SyncClientInterface asSyncClient() {
        return null;
    }

    public void shutdown() {

    }

    public void switchCredentialsProvider(CredentialsProvider newCrdsProvider) {

    }

    public Future<SQLQueryResponse> sqlQuery(SQLQueryRequest request, TableStoreCallback<SQLQueryRequest, SQLQueryResponse> callback) {
        return null;
    }
}
