package com.alicloud.openservices.tablestore;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.delivery.*;
import com.alicloud.openservices.tablestore.model.iterator.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;

public class SyncClient implements SyncClientInterface {
    public class DefaultPrepareCallback implements PrepareCallback {
        public DefaultPrepareCallback() {
        }

        @Override
        public void onPrepare() {
        }
    }
    private InternalClient internalClient;
    private PrepareCallback prepareCallback = new DefaultPrepareCallback();

    /**
     * Constructs a new {@link SyncClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     */
    public SyncClient(String endpoint, String accessKeyId,
                      String accessKeySecret, String instanceName) {
        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName);
    }

    /**
     * Constructs a new {@link SyncClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param stsToken        Sts Token.
     */
    public SyncClient(String endpoint, String accessKeyId,
                      String accessKeySecret, String instanceName, String stsToken) {
        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, null, null, stsToken);
    }

    /**
     * Constructs a new {@link SyncClient} instance using the specified TableStore Endpoint and configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param config          The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     */
    public SyncClient(String endpoint, String accessKeyId,
                      String accessKeySecret, String instanceName,
                      ClientConfiguration config) {
        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, config);
    }

    /**
     * Constructs a new {@link SyncClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param config          The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param stsToken        Sts Token.
     */
    public SyncClient(String endpoint, String accessKeyId,
                      String accessKeySecret, String instanceName,  ClientConfiguration config, String stsToken) {
        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, config, null, stsToken);
    }

    /**
     * Constructs a new {@link SyncClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint         The endpoint of the TableStore service.
     * @param accessKeyId      The Access ID for accessing the TableStore service.
     * @param accessKeySecret  The Access Key for accessing the TableStore service.
     * @param instanceName     The instance name for accessing the TableStore service.
     * @param config           The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param stsToken         Sts Token.
     * @param callbackExecutor  The thread pool for executing callbacks. Note that this thread pool will also be shut down when the client is shut down.
     */
    public SyncClient(String endpoint, String accessKeyId,
                      String accessKeySecret, String instanceName, ClientConfiguration config, String stsToken, ExecutorService callbackExecutor) {
        this.internalClient = new InternalClient(endpoint, accessKeyId, accessKeySecret, instanceName, config, callbackExecutor, stsToken);
    }

    public SyncClient(String endpoint, CredentialsProvider credsProvider, String instanceName,
                      ClientConfiguration config, ResourceManager resourceManager) {
        this.internalClient = new InternalClient(endpoint, credsProvider, instanceName, config, resourceManager);
    }

    SyncClient(InternalClient internalClient) {
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
        return this.internalClient.getEndpoint();
    }

    /**
     * Returns the name of the accessed instance
     *
     * @return instance name
     */
    public String getInstanceName() {
        return this.internalClient.getInstanceName();
    }

    @Override
    public CreateTableResponse createTable(CreateTableRequest createTableRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(createTableRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<CreateTableResponse> res = this.internalClient.createTable(createTableRequest, null);
        return waitForFuture(res);
    }

    @Override
    public ListTableResponse listTable()
        throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<ListTableResponse> res = this.internalClient.listTable(null);
        return waitForFuture(res);
    }

    @Override
    public DescribeTableResponse describeTable(DescribeTableRequest request)
        throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<DescribeTableResponse> res = this.internalClient.describeTable(request, null);
        return waitForFuture(res);
    }

    @Override
    public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest)
        throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(deleteTableRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<DeleteTableResponse> res = this.internalClient.deleteTable(deleteTableRequest, null);
        return waitForFuture(res);
    }

    @Override
    public UpdateTableResponse updateTable(UpdateTableRequest request)
        throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<UpdateTableResponse> res = this.internalClient.updateTable(request, null);
        return waitForFuture(res);
    }

    @Override
    public CreateIndexResponse createIndex(CreateIndexRequest createIndexRequest)
        throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(createIndexRequest);

        Future<CreateIndexResponse> res = this.internalClient.createIndex(createIndexRequest, null);
        return waitForFuture(res);
    }

    @Override
    public DeleteIndexResponse deleteIndex(DeleteIndexRequest deleteIndexRequest)
        throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(deleteIndexRequest);

        Future<DeleteIndexResponse> res = this.internalClient.deleteIndex(deleteIndexRequest, null);
        return waitForFuture(res);
    }

    @Override
    public AddDefinedColumnResponse addDefinedColumn(AddDefinedColumnRequest addDefinedColumnRequest)
        throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(addDefinedColumnRequest);

        Future<AddDefinedColumnResponse> res = this.internalClient.addDefinedColumn(addDefinedColumnRequest, null);
        return waitForFuture(res);
    }

    @Override
    public DeleteDefinedColumnResponse deleteDefinedColumn(DeleteDefinedColumnRequest deleteDefinedColumnRequest)
        throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(deleteDefinedColumnRequest);

        Future<DeleteDefinedColumnResponse> res = this.internalClient.deleteDefinedColumn(deleteDefinedColumnRequest, null);
        return waitForFuture(res);
    }

    @Override
    public GetRowResponse getRow(GetRowRequest getRowRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(getRowRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<GetRowResponse> res = this.internalClient.getRow(getRowRequest, null);
        return waitForFuture(res);
    }

    @Override
    public PutRowResponse putRow(PutRowRequest putRowRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(putRowRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<PutRowResponse> res = this.internalClient.putRow(putRowRequest, null);
        return waitForFuture(res);
    }

    @Override
    public UpdateRowResponse updateRow(UpdateRowRequest updateRowRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(updateRowRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<UpdateRowResponse> res = this.internalClient.updateRow(updateRowRequest, null);
        return waitForFuture(res);
    }

    @Override
    public DeleteRowResponse deleteRow(DeleteRowRequest deleteRowRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(deleteRowRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<DeleteRowResponse> res = this.internalClient.deleteRow(deleteRowRequest, null);
        return waitForFuture(res);
    }

    @Override
    public BatchGetRowResponse batchGetRow(final BatchGetRowRequest batchGetRowRequest) 
    		throws TableStoreException, ClientException 
    {
        Preconditions.checkNotNull(batchGetRowRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<BatchGetRowResponse> res = this.internalClient.batchGetRow(batchGetRowRequest, null);
        return waitForFuture(res);
    }

    @Override
    public BatchWriteRowResponse batchWriteRow(final BatchWriteRowRequest batchWriteRowRequest) 
    		throws TableStoreException, ClientException 
    {
        Preconditions.checkNotNull(batchWriteRowRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<BatchWriteRowResponse> res = this.internalClient.batchWriteRow(batchWriteRowRequest, null);
        return waitForFuture(res);
    }

    @Override
    public BulkImportResponse bulkImport(final BulkImportRequest bulkImportRequest)
            throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(bulkImportRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<BulkImportResponse> res = this.internalClient.bulkImport(bulkImportRequest, null);
        return waitForFuture(res);
    }

    @Override
    public GetRangeResponse getRange(GetRangeRequest getRangeRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(getRangeRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<GetRangeResponse> res = this.internalClient.getRange(getRangeRequest, null);
        return waitForFuture(res);
    }

    @Override
    public BulkExportResponse bulkExport(BulkExportRequest bulkExportRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(bulkExportRequest);
        Preconditions.checkNotNull(prepareCallback);
        prepareCallback.onPrepare();
        Future<BulkExportResponse> res = this.internalClient.bulkExport(bulkExportRequest, null);
        return waitForFuture(res);
    }

    @Override
    public ComputeSplitsBySizeResponse computeSplitsBySize(ComputeSplitsBySizeRequest computeSplitsBySizeRequest)
            throws TableStoreException, ClientException {
        Preconditions.checkNotNull(computeSplitsBySizeRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<ComputeSplitsBySizeResponse> res = this.internalClient.computeSplitsBySize(computeSplitsBySizeRequest, null);
        return waitForFuture(res);
    }

    @Override
    public Iterator<Row> createRangeIterator(
            RangeIteratorParameter rangeIteratorParameter) throws TableStoreException,
            ClientException {
        return new GetRangeRowIterator(this, rangeIteratorParameter);
    }

    @Override
    public Iterator<Row> createBulkExportIterator(
            RangeIteratorParameter rangeIteratorParameter) throws TableStoreException,
            ClientException {
        return new BulkExportIterator(this, rangeIteratorParameter);
    }

    @Override
    public WideColumnIterator createWideColumnIterator(GetRowRequest getRowRequest) throws TableStoreException, ClientException {
        GetRowColumnIteratorImpl getRowColumnIterator = new GetRowColumnIteratorImpl(internalClient, getRowRequest);
        if (getRowColumnIterator.isRowExistent()) {
            return new WideColumnIterator(getRowRequest.getRowQueryCriteria().getPrimaryKey(), getRowColumnIterator);
        } else {
            return null;
        }
    }

    @Override
    public ListStreamResponse listStream(ListStreamRequest listStreamRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(listStreamRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<ListStreamResponse> res = this.internalClient.listStream(listStreamRequest, null);
        return waitForFuture(res);
    }

    @Override
    public DescribeStreamResponse describeStream(DescribeStreamRequest describeStreamRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(describeStreamRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<DescribeStreamResponse> res = this.internalClient.describeStream(describeStreamRequest, null);
        return waitForFuture(res);
    }

    @Override
    public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest getShardIteratorRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(getShardIteratorRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<GetShardIteratorResponse> res = this.internalClient.getShardIterator(getShardIteratorRequest, null);
        return waitForFuture(res);
    }

    @Override
    public GetStreamRecordResponse getStreamRecord(GetStreamRecordRequest getStreamRecordRequest) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(getStreamRecordRequest);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<GetStreamRecordResponse> res = this.internalClient.getStreamRecord(getStreamRecordRequest, null);
        return waitForFuture(res);
    }

    @Override
    public CreateSearchIndexResponse createSearchIndex(CreateSearchIndexRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<CreateSearchIndexResponse> res = this.internalClient.createSearchIndex(request, null);
        return waitForFuture(res);
    }

    @Override
    public UpdateSearchIndexResponse updateSearchIndex(UpdateSearchIndexRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<UpdateSearchIndexResponse> res = this.internalClient.updateSearchIndex(request, null);
        return waitForFuture(res);
    }

    @Override
    public ListSearchIndexResponse listSearchIndex(ListSearchIndexRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<ListSearchIndexResponse> res = this.internalClient.listSearchIndex(request, null);
        return waitForFuture(res);
    }

    @Override
    public DeleteSearchIndexResponse deleteSearchIndex(DeleteSearchIndexRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<DeleteSearchIndexResponse> res = this.internalClient.deleteSearchIndex(request, null);
        return waitForFuture(res);
    }

    @Override
    public DescribeSearchIndexResponse describeSearchIndex(DescribeSearchIndexRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkStringNotNullAndEmpty(request.getTableName(), "TableName should not be null or empty.");
        Preconditions.checkStringNotNullAndEmpty(request.getIndexName(), "TableName should not be null or empty.");
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<DescribeSearchIndexResponse> res = this.internalClient.describeSearchIndex(request, null);
        return waitForFuture(res);
    }

    @Override
    public ComputeSplitsResponse computeSplits(ComputeSplitsRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<ComputeSplitsResponse> res = this.internalClient.computeSplits(request, null);
        return waitForFuture(res);
    }

    @Override
    public ParallelScanResponse parallelScan(ParallelScanRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<ParallelScanResponse> res = this.internalClient.parallelScan(request, null);
        return waitForFuture(res);
    }

    @Override
    public RowIterator createParallelScanIterator(ParallelScanRequest request) throws TableStoreException, ClientException {
        return new ParallelScanRowIterator(this, request);
    }

    @Override
    public RowIterator createSearchIterator(SearchRequest request) throws TableStoreException, ClientException {
        return new SearchRowIterator(this, request);
    }

    @Override
    public SearchResponse search(SearchRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<SearchResponse> res = this.internalClient.search(request, null);
        return waitForFuture(res);
    }

    @Override
    public StartLocalTransactionResponse startLocalTransaction(StartLocalTransactionRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<StartLocalTransactionResponse> res = this.internalClient.startLocalTransaction(request, null);
        return waitForFuture(res);
    }

    @Override
    public CommitTransactionResponse commitTransaction(CommitTransactionRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<CommitTransactionResponse> res = this.internalClient.commitTransaction(request, null);
        return waitForFuture(res);
    }

    @Override
    public AbortTransactionResponse abortTransaction(AbortTransactionRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<AbortTransactionResponse> res = this.internalClient.abortTransaction(request, null);
        return waitForFuture(res);
    }

    @Override
    public CreateDeliveryTaskResponse createDeliveryTask(CreateDeliveryTaskRequest request)
            throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(request);

        Future<CreateDeliveryTaskResponse> res = this.internalClient.createDeliveryTask(request, null);
        return waitForFuture(res);
    }

    @Override
    public DeleteDeliveryTaskResponse deleteDeliveryTask(DeleteDeliveryTaskRequest request)
            throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(request);

        Future<DeleteDeliveryTaskResponse> res = this.internalClient.deleteDeliveryTask(request, null);
        return waitForFuture(res);
    }

    @Override
    public DescribeDeliveryTaskResponse describeDeliveryTask(DescribeDeliveryTaskRequest request)
            throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(request);

        Future<DescribeDeliveryTaskResponse> res = this.internalClient.describeDeliveryTask(request, null);
        return waitForFuture(res);
    }

    @Override
    public ListDeliveryTaskResponse listDeliveryTask(ListDeliveryTaskRequest request)
            throws TableStoreException, ClientException
    {
        Preconditions.checkNotNull(request);

        Future<ListDeliveryTaskResponse> res = this.internalClient.listDeliveryTask(request, null);
        return waitForFuture(res);
    }

    @Override
    public SQLQueryResponse sqlQuery(SQLQueryRequest request) throws TableStoreException, ClientException {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(prepareCallback);

        prepareCallback.onPrepare();
        Future<SQLQueryResponse> res = this.internalClient.sqlQuery(request,null);
        return waitForFuture(res);
    }

    private <Res> Res waitForFuture(Future<Res> f) {
        try {
            return f.get(this.internalClient.getClientConfig().getSyncClientWaitFutureTimeoutInMillis(), TimeUnit.MILLISECONDS);
        } catch(InterruptedException e) {
            throw new ClientException(String.format(
                    "The thread was interrupted: %s", e.getMessage()));
        } catch(ExecutionException e) {
            throw new ClientException("The thread was aborted", e);
        } catch (TimeoutException e) {
            throw new ClientException("Wait future timeout", e);
        }
    }

    @Override
    public AsyncClientInterface asAsyncClient() {
        return new AsyncClient(this.internalClient);
    }

    public TimeseriesClient asTimeseriesClient() {
        return new TimeseriesClient(this.internalClient);
    }

    public AsyncTimeseriesClient asAsyncTimeseriesClient() {
        return new AsyncTimeseriesClient(this.internalClient);
    }

    @Override
    public void shutdown() {
        this.internalClient.shutdown();
    }

    public void setPrepareCallback(PrepareCallback cb) {
        prepareCallback = cb;
    }

    public void setCredentials(ServiceCredentials credentials) {
        internalClient.setCredentials(credentials);
    }

    @Override
    public void switchCredentialsProvider(CredentialsProvider newCrdsProvider) {
        internalClient.switchCredentialsProvider(newCrdsProvider);
    }
}
