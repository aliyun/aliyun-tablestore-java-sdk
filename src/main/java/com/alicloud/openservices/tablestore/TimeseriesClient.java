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
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link TimeseriesClient}实例。
     * @param endpoint        TableStore服务的Endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore的服务的实例名称。
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, new ClientConfiguration());
    }

    /**
     * 使用指定的TableStore Endpoint和配置构造一个新的{@link TimeseriesClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param conf            客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName, ClientConfiguration conf) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, conf, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link TimeseriesClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param stsToken        Sts Token.
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, new ClientConfiguration(), stsToken);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link TimeseriesClient}实例。
     *
     * @param endpoint        TableStore服务的endpoint。
     * @param accessKeyId     访问TableStore服务的Access ID。
     * @param accessKeySecret 访问TableStore服务的Access Key。
     * @param instanceName    访问TableStore服务的实例名称。
     * @param config          客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param stsToken        Sts Token.
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
                            ClientConfiguration config, String stsToken) {
        this(endpoint, accessKeyId, accessKeySecret, instanceName, config, stsToken, null);
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link TimeseriesClient}实例。
     *
     * @param endpoint         TableStore服务的endpoint。
     * @param accessKeyId      访问TableStore服务的Access ID。
     * @param accessKeySecret  访问TableStore服务的Access Key。
     * @param instanceName     访问TableStore服务的实例名称。
     * @param config           客户端配置信息（{@link ClientConfiguration}）。 如果传入null则使用默认配置。
     * @param stsToken         Sts Token.
     * @param callbackExecutor 执行callback的线程池，需要注意的是，client在shutdown的时候也会shutdown这个线程池。
     */
    public TimeseriesClient(String endpoint, String accessKeyId, String accessKeySecret, String instanceName,
                            ClientConfiguration config, String stsToken, ExecutorService callbackExecutor) {
        this(endpoint, new DefaultCredentialProvider(accessKeyId, accessKeySecret, stsToken), instanceName, config,
                new ResourceManager(config, callbackExecutor));
    }

    /**
     * 使用指定的TableStore Endpoint和默认配置构造一个新的{@link TimeseriesClient}实例。
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
