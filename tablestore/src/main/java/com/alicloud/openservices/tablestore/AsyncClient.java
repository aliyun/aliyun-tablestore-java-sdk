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
import com.alicloud.openservices.tablestore.model.knowledgebase.*;
import com.alicloud.openservices.tablestore.model.memory.*;

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

    // KnowledgeBase operations
    @Override
    public Future<CreateKnowledgeBaseResponse> createKnowledgeBase(CreateKnowledgeBaseRequest request, TableStoreCallback<CreateKnowledgeBaseRequest, CreateKnowledgeBaseResponse> callback) {
        return internalClient.createKnowledgeBase(request, callback);
    }

    @Override
    public Future<DescribeKnowledgeBaseResponse> describeKnowledgeBase(DescribeKnowledgeBaseRequest request, TableStoreCallback<DescribeKnowledgeBaseRequest, DescribeKnowledgeBaseResponse> callback) {
        return internalClient.describeKnowledgeBase(request, callback);
    }

    @Override
    public Future<ListKnowledgeBaseResponse> listKnowledgeBase(ListKnowledgeBaseRequest request, TableStoreCallback<ListKnowledgeBaseRequest, ListKnowledgeBaseResponse> callback) {
        return internalClient.listKnowledgeBase(request, callback);
    }

    @Override
    public Future<DeleteKnowledgeBaseResponse> deleteKnowledgeBase(DeleteKnowledgeBaseRequest request, TableStoreCallback<DeleteKnowledgeBaseRequest, DeleteKnowledgeBaseResponse> callback) {
        return internalClient.deleteKnowledgeBase(request, callback);
    }

    @Override
    public Future<UpdateKnowledgeBaseResponse> updateKnowledgeBase(UpdateKnowledgeBaseRequest request, TableStoreCallback<UpdateKnowledgeBaseRequest, UpdateKnowledgeBaseResponse> callback) {
        return internalClient.updateKnowledgeBase(request, callback);
    }

    // Document operations
    @Override
    public Future<AddDocumentsResponse> addDocuments(AddDocumentsRequest request, TableStoreCallback<AddDocumentsRequest, AddDocumentsResponse> callback) {
        return internalClient.addDocuments(request, callback);
    }

    @Override
    public Future<GetDocumentResponse> getDocument(GetDocumentRequest request, TableStoreCallback<GetDocumentRequest, GetDocumentResponse> callback) {
        return internalClient.getDocument(request, callback);
    }

    @Override
    public Future<ListDocumentsResponse> listDocuments(ListDocumentsRequest request, TableStoreCallback<ListDocumentsRequest, ListDocumentsResponse> callback) {
        return internalClient.listDocuments(request, callback);
    }

    @Override
    public Future<DeleteDocumentsResponse> deleteDocuments(DeleteDocumentsRequest request, TableStoreCallback<DeleteDocumentsRequest, DeleteDocumentsResponse> callback) {
        return internalClient.deleteDocuments(request, callback);
    }

    @Override
    public Future<UpdateDocumentResponse> updateDocument(UpdateDocumentRequest request, TableStoreCallback<UpdateDocumentRequest, UpdateDocumentResponse> callback) {
        return internalClient.updateDocument(request, callback);
    }

    // Chunk operations
    @Override
    public Future<ListChunksResponse> listChunks(ListChunksRequest request, TableStoreCallback<ListChunksRequest, ListChunksResponse> callback) {
        return internalClient.listChunks(request, callback);
    }

    @Override
    public Future<UpdateChunksResponse> updateChunks(UpdateChunksRequest request, TableStoreCallback<UpdateChunksRequest, UpdateChunksResponse> callback) {
        return internalClient.updateChunks(request, callback);
    }

    // Retrieval operations
    @Override
    public Future<RetrieveResponse> retrieve(RetrieveRequest request, TableStoreCallback<RetrieveRequest, RetrieveResponse> callback) {
        return internalClient.retrieve(request, callback);
    }

    @Override
    public Future<CreateMemoryStoreResponse> createMemoryStore(CreateMemoryStoreRequest request,
            TableStoreCallback<CreateMemoryStoreRequest, CreateMemoryStoreResponse> callback) {
        return internalClient.createMemoryStore(request, callback);
    }

    @Override
    public Future<GetMemoryStoreResponse> getMemoryStore(GetMemoryStoreRequest request,
            TableStoreCallback<GetMemoryStoreRequest, GetMemoryStoreResponse> callback) {
        return internalClient.getMemoryStore(request, callback);
    }

    @Override
    public Future<ListMemoryStoresResponse> listMemoryStores(ListMemoryStoresRequest request,
            TableStoreCallback<ListMemoryStoresRequest, ListMemoryStoresResponse> callback) {
        return internalClient.listMemoryStores(request, callback);
    }

    @Override
    public Future<UpdateMemoryStoreResponse> updateMemoryStore(UpdateMemoryStoreRequest request,
            TableStoreCallback<UpdateMemoryStoreRequest, UpdateMemoryStoreResponse> callback) {
        return internalClient.updateMemoryStore(request, callback);
    }

    @Override
    public Future<DeleteMemoryStoreResponse> deleteMemoryStore(DeleteMemoryStoreRequest request,
            TableStoreCallback<DeleteMemoryStoreRequest, DeleteMemoryStoreResponse> callback) {
        return internalClient.deleteMemoryStore(request, callback);
    }

    @Override
    public Future<AddMemoriesResponse> addMemories(AddMemoriesRequest request,
            TableStoreCallback<AddMemoriesRequest, AddMemoriesResponse> callback) {
        return internalClient.addMemories(request, callback);
    }

    @Override
    public Future<SearchMemoriesResponse> searchMemories(SearchMemoriesRequest request,
            TableStoreCallback<SearchMemoriesRequest, SearchMemoriesResponse> callback) {
        return internalClient.searchMemories(request, callback);
    }

    @Override
    public Future<ListMemoriesResponse> listMemories(ListMemoriesRequest request,
            TableStoreCallback<ListMemoriesRequest, ListMemoriesResponse> callback) {
        return internalClient.listMemories(request, callback);
    }

    @Override
    public Future<GetMemoryResponse> getMemory(GetMemoryRequest request,
            TableStoreCallback<GetMemoryRequest, GetMemoryResponse> callback) {
        return internalClient.getMemory(request, callback);
    }

    @Override
    public Future<UpdateMemoryResponse> updateMemory(UpdateMemoryRequest request,
            TableStoreCallback<UpdateMemoryRequest, UpdateMemoryResponse> callback) {
        return internalClient.updateMemory(request, callback);
    }

    @Override
    public Future<DeleteMemoryResponse> deleteMemory(DeleteMemoryRequest request,
            TableStoreCallback<DeleteMemoryRequest, DeleteMemoryResponse> callback) {
        return internalClient.deleteMemory(request, callback);
    }

    @Override
    public Future<ListMemoryStoreMessagesResponse> listMemoryStoreMessages(ListMemoryStoreMessagesRequest request,
            TableStoreCallback<ListMemoryStoreMessagesRequest, ListMemoryStoreMessagesResponse> callback) {
        return internalClient.listMemoryStoreMessages(request, callback);
    }

    @Override
    public Future<ListMemoryStoreRequestsResponse> listMemoryStoreRequests(ListMemoryStoreRequestsRequest request,
            TableStoreCallback<ListMemoryStoreRequestsRequest, ListMemoryStoreRequestsResponse> callback) {
        return internalClient.listMemoryStoreRequests(request, callback);
    }

    @Override
    public Future<GetMemoryTaskResponse> getMemoryTask(GetMemoryTaskRequest request,
            TableStoreCallback<GetMemoryTaskRequest, GetMemoryTaskResponse> callback) {
        return internalClient.getMemoryTask(request, callback);
    }

    @Override
    public Future<ListMemoryTasksResponse> listMemoryTasks(ListMemoryTasksRequest request,
            TableStoreCallback<ListMemoryTasksRequest, ListMemoryTasksResponse> callback) {
        return internalClient.listMemoryTasks(request, callback);
    }

    @Override
    public Future<ListMemoryStoreScopesResponse> listMemoryStoreScopes(ListMemoryStoreScopesRequest request,
            TableStoreCallback<ListMemoryStoreScopesRequest, ListMemoryStoreScopesResponse> callback) {
        return internalClient.listMemoryStoreScopes(request, callback);
    }

    @Override
    public Future<CreateMemoryDreamTaskResponse> createMemoryDreamTask(CreateMemoryDreamTaskRequest request,
            TableStoreCallback<CreateMemoryDreamTaskRequest, CreateMemoryDreamTaskResponse> callback) {
        return internalClient.createMemoryDreamTask(request, callback);
    }

    @Override
    public Future<GetMemoryDreamTaskResponse> getMemoryDreamTask(GetMemoryDreamTaskRequest request,
            TableStoreCallback<GetMemoryDreamTaskRequest, GetMemoryDreamTaskResponse> callback) {
        return internalClient.getMemoryDreamTask(request, callback);
    }

    @Override
    public Future<ListMemoryDreamTasksResponse> listMemoryDreamTasks(ListMemoryDreamTasksRequest request,
            TableStoreCallback<ListMemoryDreamTasksRequest, ListMemoryDreamTasksResponse> callback) {
        return internalClient.listMemoryDreamTasks(request, callback);
    }

    @Override
    public Future<CancelMemoryDreamTaskResponse> cancelMemoryDreamTask(CancelMemoryDreamTaskRequest request,
            TableStoreCallback<CancelMemoryDreamTaskRequest, CancelMemoryDreamTaskResponse> callback) {
        return internalClient.cancelMemoryDreamTask(request, callback);
    }

    @Override
    public Future<ListMemoryDreamActionsResponse> listMemoryDreamActions(ListMemoryDreamActionsRequest request,
            TableStoreCallback<ListMemoryDreamActionsRequest, ListMemoryDreamActionsResponse> callback) {
        return internalClient.listMemoryDreamActions(request, callback);
    }

    @Override
    public Future<ApplyMemoryDreamActionsResponse> applyMemoryDreamActions(ApplyMemoryDreamActionsRequest request,
            TableStoreCallback<ApplyMemoryDreamActionsRequest, ApplyMemoryDreamActionsResponse> callback) {
        return internalClient.applyMemoryDreamActions(request, callback);
    }

    @Override
    public Future<AddItemResponse> addItem(AddItemRequest request,
            TableStoreCallback<AddItemRequest, AddItemResponse> callback) {
        return internalClient.addItem(request, callback);
    }

    @Override
    public Future<ListItemsResponse> listItems(ListItemsRequest request,
            TableStoreCallback<ListItemsRequest, ListItemsResponse> callback) {
        return internalClient.listItems(request, callback);
    }

    @Override
    public Future<GetItemResponse> getItem(GetItemRequest request,
            TableStoreCallback<GetItemRequest, GetItemResponse> callback) {
        return internalClient.getItem(request, callback);
    }

    @Override
    public Future<UpdateItemResponse> updateItem(UpdateItemRequest request,
            TableStoreCallback<UpdateItemRequest, UpdateItemResponse> callback) {
        return internalClient.updateItem(request, callback);
    }

    @Override
    public Future<DeleteItemResponse> deleteItem(DeleteItemRequest request,
            TableStoreCallback<DeleteItemRequest, DeleteItemResponse> callback) {
        return internalClient.deleteItem(request, callback);
    }

    @Override
    public Future<ListItemVersionsResponse> listItemVersions(ListItemVersionsRequest request,
            TableStoreCallback<ListItemVersionsRequest, ListItemVersionsResponse> callback) {
        return internalClient.listItemVersions(request, callback);
    }

    @Override
    public Future<GetItemVersionResponse> getItemVersion(GetItemVersionRequest request,
            TableStoreCallback<GetItemVersionRequest, GetItemVersionResponse> callback) {
        return internalClient.getItemVersion(request, callback);
    }

    @Override
    public Future<RedactItemVersionResponse> redactItemVersion(RedactItemVersionRequest request,
            TableStoreCallback<RedactItemVersionRequest, RedactItemVersionResponse> callback) {
        return internalClient.redactItemVersion(request, callback);
    }

}
