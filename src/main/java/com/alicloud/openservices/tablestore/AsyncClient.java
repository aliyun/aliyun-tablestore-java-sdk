package com.alicloud.openservices.tablestore;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;

import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;

public class AsyncClient implements AsyncClientInterface {

    private InternalClient internalClient;

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, null, null, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param stsToken        Sts Token.
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, null, null, stsToken);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName,
                       ClientConfiguration config) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, null, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param stsToken        Sts Token.
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName,
                       ClientConfiguration config, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, null, stsToken);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param config           客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param callbackExecutor 用于执行用户在调用异步接口时传入的Callback。如果传入null则使用默认配置(
     *                         线程数与CPU核数相同的线程池)。
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName,
                       ClientConfiguration config, ExecutorService callbackExecutor) {
        internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, config, callbackExecutor);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link AsyncClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param config           客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param callbackExecutor 用于执行用户在调用异步接口时传入的Callback。如果传入null则使用默认配置(
     *                         线程数与CPU核数相同的线程池)。
     * @param stsToken         Sts Token.
     */
    public AsyncClient(String endpoint, String accessKeyId,
                       String accessKeySecret, String instanceName,
                       ClientConfiguration config, ExecutorService callbackExecutor, String stsToken) {
        internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, config, callbackExecutor, stsToken);
    }

    AsyncClient(InternalClient internalClient) {
        this.internalClient = internalClient;
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.internalClient.setExtraHeaders(extraHeaders);
    }

    /**
     * 返回访问的TableStore Endpoint。
     *
     * @return TableStore Endpoint。
     */
    public String getEndpoint() {
        return internalClient.getEndpoint();
    }

    /**
     * 返回访问的实例的名称
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
    public SyncClientInterface asSyncClient() {
        return new SyncClient(this.internalClient);
    }

    @Override
    public void shutdown() {
        internalClient.shutdown();
    }

    @Override
    public void switchCredentialsProvider(CredentialsProvider newCrdsProvider) {
        internalClient.switchCredentialsProvider(newCrdsProvider);
    }
}
