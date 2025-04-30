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
import java.util.concurrent.*;

public class TimeseriesClient implements TimeseriesClientInterface {

    private InternalClient internalClient;
    private Cache<String, Long> timeseriesMetaCache;

    /**
     * Constructs a new {@link TimeseriesClient} instance using the specified TableStore Endpoint and default configuration.
     * @param endpoint        The Endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name of the TableStore service.
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, new ClientConfiguration());
    }

    /**
     * Constructs a new {@link TimeseriesClient} instance using the specified TableStore Endpoint and configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param conf            The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName, ClientConfiguration conf) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, conf, null);
    }

    /**
     * Constructs a new {@link TimeseriesClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param stsToken        Sts Token.
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, new ClientConfiguration(), stsToken);
    }

    /**
     * Constructs a new {@link TimeseriesClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint        The endpoint of the TableStore service.
     * @param accessKeyId     The Access ID for accessing the TableStore service.
     * @param accessKeySecret The Access Key for accessing the TableStore service.
     * @param instanceName    The instance name for accessing the TableStore service.
     * @param config          The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param stsToken        The Sts Token.
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
                            ClientConfiguration config, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, stsToken, null);
    }

    /**
     * Constructs a new {@link TimeseriesClient} instance using the specified TableStore Endpoint and default configuration.
     *
     * @param endpoint         The endpoint of the TableStore service.
     * @param accessKeyId      The Access ID for accessing the TableStore service.
     * @param accessKeySecret  The Access Key for accessing the TableStore service.
     * @param instanceName     The instance name for accessing the TableStore service.
     * @param config           The client configuration information ({@link ClientConfiguration}). If null is passed, the default configuration will be used.
     * @param stsToken         The Sts Token.
     * @param callbackExecutor The thread pool for executing callbacks. Note that this thread pool will also be shut down when the client is shut down.
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
                            ClientConfiguration config, String stsToken, ExecutorService callbackExecutor) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret, stsToken), instanceName, config,
                new ResourceManager(config, callbackExecutor));
    }

    /**
     * Constructs a new {@link TimeseriesClient} instance using the specified TableStore Endpoint and default configurations.
     */
    public TimeseriesClient(String endpoint, CredentialsProvider credsProvider, String instanceName,
                            ClientConfiguration config, ResourceManager resourceManager) {
        this(new InternalClient(endpoint, credsProvider, instanceName, config, resourceManager));
    }

    TimeseriesClient(InternalClient internalClient) {
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
    public CreateTimeseriesTableResponse createTimeseriesTable(CreateTimeseriesTableRequest request)
    {
        Future<CreateTimeseriesTableResponse> resp = internalClient.createTimeseriesTable(request, null);
        return waitForFuture(resp);
    }

    @Override
    public ListTimeseriesTableResponse listTimeseriesTable()
    {
        Future<ListTimeseriesTableResponse> resp = internalClient.listTimeseriesTable(null);
        return waitForFuture(resp);
    }

    @Override
    public DeleteTimeseriesTableResponse deleteTimeseriesTable(DeleteTimeseriesTableRequest request)
    {
        Future<DeleteTimeseriesTableResponse> resp = internalClient.deleteTimeseriesTable(request, null);
        return waitForFuture(resp);
    }

    @Override
    public DescribeTimeseriesTableResponse describeTimeseriesTable(DescribeTimeseriesTableRequest request)
    {
        Future<DescribeTimeseriesTableResponse> resp = internalClient.describeTimeseriesTable(request, null);
        return waitForFuture(resp);
    }

    @Override
    public UpdateTimeseriesTableResponse updateTimeseriesTable(UpdateTimeseriesTableRequest request)
    {
        Future<UpdateTimeseriesTableResponse> resp = internalClient.updateTimeseriesTable(request, null);
        return waitForFuture(resp);
    }

    @Override
    public PutTimeseriesDataResponse putTimeseriesData(PutTimeseriesDataRequest request)
            throws TableStoreException, ClientException {
        Future<PutTimeseriesDataResponse> resp = internalClient.putTimeseriesData(request, null);
        return waitForFuture(resp);
    }

    @Override
    public GetTimeseriesDataResponse getTimeseriesData(GetTimeseriesDataRequest request)
        throws TableStoreException, ClientException {
        Future<GetTimeseriesDataResponse> resp = internalClient.getTimeseriesData(request, null);
        return waitForFuture(resp);
    }

    @Override
    public QueryTimeseriesMetaResponse queryTimeseriesMeta(QueryTimeseriesMetaRequest request)
        throws TableStoreException, ClientException {
        Future<QueryTimeseriesMetaResponse> resp = internalClient.queryTimeseriesMeta(request, null);
        return waitForFuture(resp);
    }

    @Override
    public UpdateTimeseriesMetaResponse updateTimeseriesMeta(UpdateTimeseriesMetaRequest request)
            throws TableStoreException, ClientException {
        Future<UpdateTimeseriesMetaResponse> resp = internalClient.updateTimeseriesMeta(request, null);
        return waitForFuture(resp);
    }

    @Override
    public DeleteTimeseriesMetaResponse deleteTimeseriesMeta(DeleteTimeseriesMetaRequest request)
            throws TableStoreException, ClientException {
        Future<DeleteTimeseriesMetaResponse> resp = internalClient.deleteTimeseriesMeta(request, null);
        return waitForFuture(resp);
    }

    @Override
    public SplitTimeseriesScanTaskResponse splitTimeseriesScanTask(SplitTimeseriesScanTaskRequest request)
            throws TableStoreException, ClientException {
        Future<SplitTimeseriesScanTaskResponse> resp = internalClient.splitTimeseriesScanTask(request, null);
        return waitForFuture(resp);
    }

    @Override
    public ScanTimeseriesDataResponse scanTimeseriesData(ScanTimeseriesDataRequest request)
            throws TableStoreException, ClientException {
        Future<ScanTimeseriesDataResponse> resp = internalClient.scanTimeseriesData(request, null);
        return waitForFuture(resp);
    }

    @Override
    public CreateTimeseriesAnalyticalStoreResponse createTimeseriesAnalyticalStore(CreateTimeseriesAnalyticalStoreRequest request)
            throws TableStoreException, ClientException {
        Future<CreateTimeseriesAnalyticalStoreResponse> resp = internalClient.createTimeseriesAnalyticalStore(request, null);
        return waitForFuture(resp);
    }

    @Override
    public DeleteTimeseriesAnalyticalStoreResponse deleteTimeseriesAnalyticalStore(DeleteTimeseriesAnalyticalStoreRequest request)
            throws TableStoreException, ClientException {
        Future<DeleteTimeseriesAnalyticalStoreResponse> resp = internalClient.deleteTimeseriesAnalyticalStore(request, null);
        return waitForFuture(resp);
    }

    @Override
    public DescribeTimeseriesAnalyticalStoreResponse describeTimeseriesAnalyticalStore(DescribeTimeseriesAnalyticalStoreRequest request)
            throws TableStoreException, ClientException {
        Future<DescribeTimeseriesAnalyticalStoreResponse> resp = internalClient.describeTimeseriesAnalyticalStore(request, null);
        return waitForFuture(resp);
    }

    @Override
    public UpdateTimeseriesAnalyticalStoreResponse updateTimeseriesAnalyticalStore(UpdateTimeseriesAnalyticalStoreRequest request)
            throws TableStoreException, ClientException {
        Future<UpdateTimeseriesAnalyticalStoreResponse> resp = internalClient.updateTimeseriesAnalyticalStore(request, null);
        return waitForFuture(resp);
    }

    @Override
    public CreateTimeseriesLastpointIndexResponse createTimeseriesLastpointIndex(CreateTimeseriesLastpointIndexRequest request)
            throws TableStoreException, ClientException {
        Future<CreateTimeseriesLastpointIndexResponse> resp = internalClient.createTimeseriesLastpointIndex(request, null);
        return waitForFuture(resp);
    }

    @Override
    public DeleteTimeseriesLastpointIndexResponse deleteTimeseriesLastpointIndex(DeleteTimeseriesLastpointIndexRequest request)
            throws TableStoreException, ClientException {
        Future<DeleteTimeseriesLastpointIndexResponse> resp = internalClient.deleteTimeseriesLastpointIndex(request, null);
        return waitForFuture(resp);
    }

    private <Res> Res waitForFuture(Future<Res> f) {
        try {
            return f.get(this.internalClient.getClientConfig().getSyncClientWaitFutureTimeoutInMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new ClientException(String.format(
                "The thread was interrupted: %s", e.getMessage()));
        } catch (ExecutionException e) {
            throw new ClientException("The thread was aborted", e);
        } catch (TimeoutException e) {
            throw new ClientException("Wait future timeout", e);
        }
    }

    public void shutdown() {
        this.internalClient.shutdown();
    }

    public SyncClient asSyncClient() {
        return new SyncClient(this.internalClient);
    }

    public AsyncClient asAsyncClient() {
        return new AsyncClient(this.internalClient);
    }

    public AsyncTimeseriesClient asAsyncTimeseriesClient() {
        return new AsyncTimeseriesClient(this.internalClient);
    }
}
