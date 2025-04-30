package com.alicloud.openservices.tablestore;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;

import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.delivery.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;

public class AsyncClient implements AsyncClientInterface {

    private InternalClient internalClient;

    /**
     * Constructs a new {@link AsyncClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The name of the instance for accessing the TableStore service.
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, null, null, null);
    }

    /**
     * Constructs a new {@link AsyncClient} instance with the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param stsToken        Sts Token.
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, null, null, stsToken);
    }

    /**
     * Constructs a new {@link AsyncClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param config          The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName,
                       ClientConfiguration config) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, null, null);
    }

    /**
     * Constructs a new {@link AsyncClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param config          The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param stsToken        Sts Token.
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName,
                       ClientConfiguration config, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, null, stsToken);
    }

    /**
     * Constructs a new {@link AsyncClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param config          The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param callbackExecutor Used to execute the Callback provided by the user when calling the asynchronous interface. If null is passed, the default configuration will be used 
     *                         (a thread pool with the number of threads equal to the number of CPU cores).
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName,
                       ClientConfiguration config, ExecutorService callbackExecutor) {
        internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, config, callbackExecutor);
    }

    /**
     * Constructs a new {@link AsyncClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     Access ID for accessing the TableStore service.
     * @param accessKeySecret Access Key for accessing the TableStore service.
     * @param instanceName    Instance name for accessing the TableStore service.
     * @param config          Client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param callbackExecutor Used to execute the Callback provided by users when calling asynchronous interfaces. If null is passed, the default configuration will be used
     *                         (a thread pool with the number of threads equal to the number of CPU cores).
     * @param stsToken         Sts Token.
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName,
                       ClientConfiguration config, ExecutorService callbackExecutor, String stsToken) {
        internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, config, callbackExecutor, stsToken);
    }

    public AsyncClient(String endpoint, CredentialsProvider credsProvider, String instanceName,
                      ClientConfiguration config, ResourceManager resourceManager) {
        internalClient = new InternalClient(endpoint, credsProvider, instanceName, config, resourceManager);
    }

    AsyncClient(InternalClient internalClient) {
        this.internalClient = internalClient;
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.internalClient.setExtraHeaders(extraHeaders);
    }

    /**
     * Returns the TableStore Endpoint being accessed.
     *
     * @return TableStore Endpoint.
     */
    public String getEndpoint() {
        return internalClient.getEndpoint();
    }

    /**
     * Returns the name of the accessed instance
     *
     * @return instance name
     */
    public String getInstanceName() {
        return internalClient.getInstanceName();
    }

    @Override
    public Future<ListTableResponse> listTable(
        TableStoreCallback<ListTableRequest, ListTableResponse> callback)
    {
        return internalClient.listTable(callback);
    }

    @Override
    public Future<CreateTableResponse> createTable(
        CreateTableRequest request,
        TableStoreCallback<CreateTableRequest, CreateTableResponse> callback)
    {
        return internalClient.createTable(request, callback);
    }

    @Override
    public Future<DescribeTableResponse> describeTable(
        DescribeTableRequest request,
        TableStoreCallback<DescribeTableRequest, DescribeTableResponse> callback)
    {
        return internalClient.describeTable(request, callback);
    }

    @Override
    public Future<DeleteTableResponse> deleteTable(
        DeleteTableRequest request,
        TableStoreCallback<DeleteTableRequest, DeleteTableResponse> callback)
    {
        return internalClient.deleteTable(request, callback);
    }

    @Override
    public Future<UpdateTableResponse> updateTable(
        UpdateTableRequest request,
        TableStoreCallback<UpdateTableRequest, UpdateTableResponse> callback)
    {
        return internalClient.updateTable(request, callback);
    }

    @Override
    public Future<CreateIndexResponse> createIndex(
        CreateIndexRequest request,
        TableStoreCallback<CreateIndexRequest, CreateIndexResponse> callback)
    {
        return internalClient.createIndex(request, callback);
    }

    @Override
    public Future<DeleteIndexResponse> deleteIndex(
        DeleteIndexRequest request,
        TableStoreCallback<DeleteIndexRequest, DeleteIndexResponse> callback)
    {
        return internalClient.deleteIndex(request, callback);
    }

    @Override
    public Future<AddDefinedColumnResponse> addDefinedColumn(
            AddDefinedColumnRequest request,
            TableStoreCallback<AddDefinedColumnRequest, AddDefinedColumnResponse> callback)
    {
        return internalClient.addDefinedColumn(request, callback);
    }

    @Override
    public Future<DeleteDefinedColumnResponse> deleteDefinedColumn(
            DeleteDefinedColumnRequest request,
            TableStoreCallback<DeleteDefinedColumnRequest, DeleteDefinedColumnResponse> callback)
    {
        return internalClient.deleteDefinedColumn(request, callback);
    }

    @Override
    public Future<GetRowResponse> getRow(GetRowRequest request,
    		TableStoreCallback<GetRowRequest, GetRowResponse> callback)
    {
        return internalClient.getRow(request, callback);
    }

    @Override
    public Future<PutRowResponse> putRow(
    		PutRowRequest request,
    		TableStoreCallback<PutRowRequest, PutRowResponse> callback)
    {
        return internalClient.putRow(request, callback);
    }

    @Override
    public Future<UpdateRowResponse> updateRow(
            UpdateRowRequest request,
            TableStoreCallback<UpdateRowRequest, UpdateRowResponse> callback)
    {
        return internalClient.updateRow(request, callback);
    }

    @Override
    public Future<DeleteRowResponse> deleteRow(
    		DeleteRowRequest request,
            TableStoreCallback<DeleteRowRequest, DeleteRowResponse> callback)
    {
        return internalClient.deleteRow(request, callback);
    }

    @Override
    public Future<BatchGetRowResponse> batchGetRow(
            BatchGetRowRequest request,
            TableStoreCallback<BatchGetRowRequest, BatchGetRowResponse> callback)
    {
        return internalClient.batchGetRowInternal(request, callback);
    }

    @Override
    public Future<BatchWriteRowResponse> batchWriteRow(
            BatchWriteRowRequest request,
            TableStoreCallback<BatchWriteRowRequest, BatchWriteRowResponse> callback)
    {
        return internalClient.batchWriteRow(request, callback);
    }

    @Override
    public Future<GetRangeResponse> getRange(GetRangeRequest request,
    		TableStoreCallback<GetRangeRequest, GetRangeResponse> callback)
    {
        return internalClient.getRangeInternal(request, callback);
    }

    @Override
    public Future<BulkExportResponse> bulkExport(BulkExportRequest request,
                                             TableStoreCallback<BulkExportRequest, BulkExportResponse> callback)
    {
        return internalClient.bulkExportInternal(request, callback);
    }

    @Override
    public Future<BulkImportResponse> bulkImport(BulkImportRequest request,
                                                 TableStoreCallback<BulkImportRequest, BulkImportResponse> callback)
    {
        return internalClient.bulkImport(request, callback);
    }
    
    @Override
    public Future<ComputeSplitsBySizeResponse> computeSplitsBySize(
            ComputeSplitsBySizeRequest request,
            TableStoreCallback<ComputeSplitsBySizeRequest, ComputeSplitsBySizeResponse> callback) {
        return internalClient.computeSplitsBySize(request, callback);
    }

    @Override
    public Future<ListStreamResponse> listStream(ListStreamRequest request, TableStoreCallback<ListStreamRequest, ListStreamResponse> callback) {
        return internalClient.listStream(request, callback);
    }

    @Override
    public Future<DescribeStreamResponse> describeStream(DescribeStreamRequest request, TableStoreCallback<DescribeStreamRequest, DescribeStreamResponse> callback) {
        return internalClient.describeStream(request, callback);
    }

    @Override
    public Future<GetShardIteratorResponse> getShardIterator(GetShardIteratorRequest request, TableStoreCallback<GetShardIteratorRequest, GetShardIteratorResponse> callback) {
        return internalClient.getShardIterator(request, callback);
    }

    @Override
    public Future<GetStreamRecordResponse> getStreamRecord(GetStreamRecordRequest request, TableStoreCallback<GetStreamRecordRequest, GetStreamRecordResponse> callback) {
        return internalClient.getStreamRecord(request, callback);
    }

    @Override
    public Future<CreateSearchIndexResponse> createSearchIndex(CreateSearchIndexRequest request, TableStoreCallback<CreateSearchIndexRequest, CreateSearchIndexResponse> callback) {
        return internalClient.createSearchIndex(request, callback);
    }

    @Override
    public Future<UpdateSearchIndexResponse> updateSearchIndex(UpdateSearchIndexRequest request, TableStoreCallback<UpdateSearchIndexRequest, UpdateSearchIndexResponse> callback) {
        return internalClient.updateSearchIndex(request, callback);
    }

    @Override
    public Future<ListSearchIndexResponse> listSearchIndex(ListSearchIndexRequest request, TableStoreCallback<ListSearchIndexRequest, ListSearchIndexResponse> callback) {
        return internalClient.listSearchIndex(request, callback);
    }

    @Override
    public Future<DeleteSearchIndexResponse> deleteSearchIndex(DeleteSearchIndexRequest request, TableStoreCallback<DeleteSearchIndexRequest, DeleteSearchIndexResponse> callback) {
        return internalClient.deleteSearchIndex(request, callback);
    }

    @Override
    public Future<DescribeSearchIndexResponse> describeSearchIndex(DescribeSearchIndexRequest request, TableStoreCallback<DescribeSearchIndexRequest, DescribeSearchIndexResponse> callback) {
        return internalClient.describeSearchIndex(request, callback);
    }

    @Override
    public Future<ComputeSplitsResponse> computeSplits(ComputeSplitsRequest request, TableStoreCallback<ComputeSplitsRequest, ComputeSplitsResponse> callback) {
        return internalClient.computeSplits(request, callback);
    }

    @Override
    public Future<ParallelScanResponse> parallelScan(ParallelScanRequest request,
        TableStoreCallback<ParallelScanRequest, ParallelScanResponse> callback) {
        return internalClient.parallelScan(request, callback);
    }

    @Override
    public Future<SearchResponse> search(SearchRequest request, TableStoreCallback<SearchRequest, SearchResponse> callback) {
        return internalClient.search(request, callback);
    }

    @Override
    public Future<StartLocalTransactionResponse> startLocalTransaction(StartLocalTransactionRequest request, TableStoreCallback<StartLocalTransactionRequest, StartLocalTransactionResponse> callback) {
        return internalClient.startLocalTransaction(request, callback);
    }

    @Override
    public Future<CommitTransactionResponse> commitTransaction(CommitTransactionRequest request, TableStoreCallback<CommitTransactionRequest, CommitTransactionResponse> callback) {
        return internalClient.commitTransaction(request, callback);
    }

    @Override
    public Future<AbortTransactionResponse> abortTransaction(AbortTransactionRequest request, TableStoreCallback<AbortTransactionRequest, AbortTransactionResponse> callback) {
        return internalClient.abortTransaction(request, callback);
    }

    @Override
    public Future<CreateDeliveryTaskResponse> createDeliveryTask(CreateDeliveryTaskRequest request, TableStoreCallback<CreateDeliveryTaskRequest, CreateDeliveryTaskResponse> callback) {
        return internalClient.createDeliveryTask(request, callback);
    }

    @Override
    public Future<DeleteDeliveryTaskResponse> deleteDeliveryTask(DeleteDeliveryTaskRequest request, TableStoreCallback<DeleteDeliveryTaskRequest, DeleteDeliveryTaskResponse> callback) {
        return internalClient.deleteDeliveryTask(request, callback);
    }

    @Override
    public Future<DescribeDeliveryTaskResponse> describeDeliveryTask(DescribeDeliveryTaskRequest request, TableStoreCallback<DescribeDeliveryTaskRequest, DescribeDeliveryTaskResponse> callback) {
        return internalClient.describeDeliveryTask(request, callback);
    }

    @Override
    public Future<ListDeliveryTaskResponse> listDeliveryTask(ListDeliveryTaskRequest request, TableStoreCallback<ListDeliveryTaskRequest, ListDeliveryTaskResponse> callback) {
        return internalClient.listDeliveryTask(request, callback);
    }

    @Override
    public SyncClientInterface asSyncClient() {
        return new SyncClient(this.internalClient);
    }

    public TimeseriesClient asTimeseriesClient() {
        return new TimeseriesClient(this.internalClient);
    }

    public AsyncTimeseriesClient asAsyncTimeseriesClient() {
        return new AsyncTimeseriesClient(this.internalClient);
    }

    @Override
    public void shutdown() {
        internalClient.shutdown();
    }

    @Override
    public void switchCredentialsProvider(CredentialsProvider newCrdsProvider) {
        internalClient.switchCredentialsProvider(newCrdsProvider);
    }

    @Override
    public Future<SQLQueryResponse> sqlQuery(SQLQueryRequest request, TableStoreCallback<SQLQueryRequest, SQLQueryResponse> callback) {
        return internalClient.sqlQuery(request, callback);
    }
}
