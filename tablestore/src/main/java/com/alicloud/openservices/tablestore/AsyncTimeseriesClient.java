package com.alicloud.openservices.tablestore;

import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentialProvider;
import com.alicloud.openservices.tablestore.core.utils.Preconditions;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AsyncTimeseriesClient implements AsyncTimeseriesClientInterface {

    private InternalClient internalClient;
    private Cache<String, Long> timeseriesMetaCache;

    /**
     * Constructs a new {@link AsyncTimeseriesClient} instance using the specified TableStore Endpoint and default configuration.
     * @param endpoint        The Endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name of the TableStore service to access.
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, new ClientConfiguration());
    }

    /**
     * Constructs a new {@link AsyncTimeseriesClient} instance using the specified TableStore Endpoint and configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param conf            The client configuration information ({@link ClientConfiguration}). If null is passed, default configuration will be used.
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName, ClientConfiguration conf) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, conf, null);
    }

    /**
     * Constructs a new {@link AsyncTimeseriesClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param stsToken        The Sts Token.
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, new ClientConfiguration(), stsToken);
    }

    /**
     * Constructs a new {@link AsyncTimeseriesClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param config          The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param stsToken        The Sts Token.
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
                            ClientConfiguration config, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, stsToken, null);
    }

    /**
     * Constructs a new {@link AsyncTimeseriesClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint         The endpoint of the TableStore service.
     * @param accessKeyId      The Access ID for accessing the TableStore service.
     * @param accessKeySecret  The Access Key for accessing the TableStore service.
     * @param instanceName     The instance name for accessing the TableStore service.
     * @param config           Client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param stsToken         Sts Token.
     * @param callbackExecutor The thread pool for executing callbacks. Note that when the client is shut down, this thread pool will also be shut down.
     */
    public AsyncTimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
                            ClientConfiguration config, String stsToken, ExecutorService callbackExecutor) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret, stsToken), instanceName,
                config, new ResourceManager(config, callbackExecutor));
    }

    /**
     * Constructs a new {@link AsyncTimeseriesClient} instance using the specified TableStore Endpoint and default configurations.
     */
    public AsyncTimeseriesClient(String endpoint, CredentialsProvider credsProvider, String instanceName,
                                 ClientConfiguration config, ResourceManager resourceManager) {
        this(new InternalClient(endpoint, credsProvider, instanceName, config, resourceManager));
    }

    AsyncTimeseriesClient(InternalClient internalClient) {
        Preconditions.checkNotNull(internalClient);
        this.internalClient = internalClient;
        if (this.internalClient.getTimeseriesMetaCache() != null) {
            this.timeseriesMetaCache = this.internalClient.getTimeseriesMetaCache();
            return;
        }
        ClientConfiguration config = this.internalClient.getClientConfig();
        if (config.getTimeseriesConfiguration() == null) {
            config.setTimeseriesConfiguration(new TimeseriesConfiguration());
        }
        this.timeseriesMetaCache = CacheBuilder.newBuilder()
                .maximumWeight(config.getTimeseriesConfiguration().getMetaCacheMaxDataSize())
                .expireAfterAccess(config.getTimeseriesConfiguration().getMetaCacheExpireTimeAfterAccessInSec(), TimeUnit.SECONDS)
                .weigher(new Weigher<String, Long>() {
                    @Override
                    public int weigh(String key, Long value) {
                        return key.length() + 48 + 16; // add some overhead
                    }
                }).build();
        this.internalClient.setTimeseriesMetaCache(timeseriesMetaCache);
    }

    public void setExtraHeaders(Map<String, String> extraHeaders) {
        this.internalClient.setExtraHeaders(extraHeaders);
    }

    @Override
    public Future<CreateTimeseriesTableResponse> createTimeseriesTable(CreateTimeseriesTableRequest request,
                                                                       TableStoreCallback<CreateTimeseriesTableRequest, CreateTimeseriesTableResponse> callback)
    {
        return internalClient.createTimeseriesTable(request, callback);
    }

    @Override
    public Future<ListTimeseriesTableResponse> listTimeseriesTable(TableStoreCallback<ListTimeseriesTableRequest, ListTimeseriesTableResponse> callback)
    {
        return internalClient.listTimeseriesTable(callback);
    }

    @Override
    public Future<DeleteTimeseriesTableResponse> deleteTimeseriesTable(DeleteTimeseriesTableRequest request,
                                                                       TableStoreCallback<DeleteTimeseriesTableRequest, DeleteTimeseriesTableResponse> callback)
    {
        return internalClient.deleteTimeseriesTable(request, callback);
    }

    @Override
    public Future<DescribeTimeseriesTableResponse> describeTimeseriesTable(DescribeTimeseriesTableRequest request,
                                                                           TableStoreCallback<DescribeTimeseriesTableRequest, DescribeTimeseriesTableResponse> callback)
    {
        return internalClient.describeTimeseriesTable(request, callback);
    }

    @Override
    public Future<UpdateTimeseriesTableResponse> updateTimeseriesTable(UpdateTimeseriesTableRequest request,
                                                                       TableStoreCallback<UpdateTimeseriesTableRequest, UpdateTimeseriesTableResponse> callback)
    {
        return internalClient.updateTimeseriesTable(request, callback);
    }

    @Override
    public Future<PutTimeseriesDataResponse> putTimeseriesData(PutTimeseriesDataRequest request,
                                                               TableStoreCallback<PutTimeseriesDataRequest, PutTimeseriesDataResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.putTimeseriesData(request, callback);
    }

    @Override
    public Future<GetTimeseriesDataResponse> getTimeseriesData(GetTimeseriesDataRequest request,
                                                               TableStoreCallback<GetTimeseriesDataRequest, GetTimeseriesDataResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.getTimeseriesData(request, callback);
    }

    @Override
    public Future<QueryTimeseriesMetaResponse> queryTimeseriesMeta(QueryTimeseriesMetaRequest request,
                                                                   TableStoreCallback<QueryTimeseriesMetaRequest, QueryTimeseriesMetaResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.queryTimeseriesMeta(request, callback);
    }

    @Override
    public Future<UpdateTimeseriesMetaResponse> updateTimeseriesMeta(UpdateTimeseriesMetaRequest request,
                                                                     TableStoreCallback<UpdateTimeseriesMetaRequest, UpdateTimeseriesMetaResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.updateTimeseriesMeta(request, callback);
    }

    @Override
    public Future<DeleteTimeseriesMetaResponse> deleteTimeseriesMeta(DeleteTimeseriesMetaRequest request,
                                                                     TableStoreCallback<DeleteTimeseriesMetaRequest, DeleteTimeseriesMetaResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.deleteTimeseriesMeta(request, callback);
    }

    @Override
    public Future<SplitTimeseriesScanTaskResponse> splitTimeseriesScanTask(SplitTimeseriesScanTaskRequest request,
                                                                           TableStoreCallback<SplitTimeseriesScanTaskRequest, SplitTimeseriesScanTaskResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.splitTimeseriesScanTask(request, callback);
    }

    @Override
    public Future<ScanTimeseriesDataResponse> scanTimeseriesData(ScanTimeseriesDataRequest request,
                                                                 TableStoreCallback<ScanTimeseriesDataRequest, ScanTimeseriesDataResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.scanTimeseriesData(request, callback);
    }

    @Override
    public Future<CreateTimeseriesAnalyticalStoreResponse> createTimeseriesAnalyticalStore(CreateTimeseriesAnalyticalStoreRequest request,
                                                                                           TableStoreCallback<CreateTimeseriesAnalyticalStoreRequest, CreateTimeseriesAnalyticalStoreResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.createTimeseriesAnalyticalStore(request, callback);
    }

    @Override
    public Future<DeleteTimeseriesAnalyticalStoreResponse> deleteTimeseriesAnalyticalStore(DeleteTimeseriesAnalyticalStoreRequest request,
                                                                                           TableStoreCallback<DeleteTimeseriesAnalyticalStoreRequest, DeleteTimeseriesAnalyticalStoreResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.deleteTimeseriesAnalyticalStore(request, callback);
    }

    @Override
    public Future<DescribeTimeseriesAnalyticalStoreResponse> describeTimeseriesAnalyticalStore(DescribeTimeseriesAnalyticalStoreRequest request,
                                                                                               TableStoreCallback<DescribeTimeseriesAnalyticalStoreRequest, DescribeTimeseriesAnalyticalStoreResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.describeTimeseriesAnalyticalStore(request, callback);
    }

    @Override
    public Future<UpdateTimeseriesAnalyticalStoreResponse> updateTimeseriesAnalyticalStore(UpdateTimeseriesAnalyticalStoreRequest request,
                                                                                           TableStoreCallback<UpdateTimeseriesAnalyticalStoreRequest, UpdateTimeseriesAnalyticalStoreResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.updateTimeseriesAnalyticalStore(request, callback);
    }

    @Override
    public Future<CreateTimeseriesLastpointIndexResponse> createTimeseriesLastpointIndex(CreateTimeseriesLastpointIndexRequest request,
                                                                                         TableStoreCallback<CreateTimeseriesLastpointIndexRequest, CreateTimeseriesLastpointIndexResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.createTimeseriesLastpointIndex(request, callback);
    }

    @Override
    public Future<DeleteTimeseriesLastpointIndexResponse> deleteTimeseriesLastpointIndex(DeleteTimeseriesLastpointIndexRequest request,
                                                                                         TableStoreCallback<DeleteTimeseriesLastpointIndexRequest, DeleteTimeseriesLastpointIndexResponse> callback)
            throws TableStoreException, ClientException {
        return internalClient.deleteTimeseriesLastpointIndex(request, callback);
    }

    public void shutdown() {
        this.internalClient.shutdown();
    }

    @Override
    public TimeseriesClientInterface asTimeseriesClientInterface() {
        return new TimeseriesClient(this.internalClient);
    }

    public SyncClient asSyncClient() {
        return new SyncClient(this.internalClient);
    }

    public AsyncClient asAsyncClient() {
        return new AsyncClient(this.internalClient);
    }

    public TimeseriesClient asTimeseriesClient() {
        return new TimeseriesClient(this.internalClient);
    }
}
